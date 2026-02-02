# Written Response

---

## 1. How I approached the data modeling problem

Three layers: staging cleans, intermediate transforms, marts serve.

**Staging** — what basic cleaning is needed, and what simple transformations enrich the data at source level? Type casting, casing normalization, and parsing `esim_package` into structured columns (`package_data_gigabytes`, `package_validity_days`, `is_unlimited_package`). One model per source table. I establish the grain and primary key at this layer — for orders that's `order_status_key` (a surrogate key per status-change row), for users it's `user_id`. Tests at the source level validate what the source system should guarantee (`not_null` on IDs, `unique` on users); issues that need downstream handling (like multi-row orders) are left for the next layer.

**Intermediate** — abstract away complexity so the final model stays clean. `int_orders_latest` resolves the append-only status history to one row per order using the most recent `updated_at`, making `order_id` a unique primary key. `int_orders_enriched` joins in user attributes, country names, and USD-converted amounts. This is where the population is cleaned up, the grain changes, and enrichment happens — keeping that logic out of the mart.

**Marts** — stakeholder-facing tables, easy to understand and fed directly into BI tools where analysis is completed.

- `orders` — one row per order with customer sequencing, denormalized user attributes, and USD amounts. Includes all statuses (completed, refunded, failed) so downstream consumers choose their own filter. Uses two sequencing namespaces: **attempts** (all orders) for funnel analysis, and **purchases** (completed + refunded only) for lifecycle and retention.
- `customers` — one row per user with aggregated order counts, revenue, repeat purchase flags, and cohort assignment. Every business question in the scenario ("how often do customers return?", "how long until they buy again?") is easier to answer from a pre-aggregated customer table than ad-hoc queries against orders.

Final tests at the mart level confirm the output does what we want — `unique` on `order_id` (which couldn't be tested until after deduplication), `accepted_values` on `order_status`, `relationships` back to users. Both marts use incremental materialization with merge strategy, partitioning, and clustering to optimize slot usage on BigQuery — full table scans on every run would be wasteful at production scale.

---

## 2. Key assumptions

**Refunded orders are purchases.** A refund means the customer signed up, selected a plan, and completed payment. The refund is a post-purchase event. Excluding refunds from the purchase sequence would undercount engagement and distort retention metrics. Revenue analysis can filter on `order_is_completed`.

**Most recent status wins.** The source contains multiple rows per order (one per status change). I assume the row with the latest `updated_at` represents the current state. This is standard for append-only event patterns but would need validation with the source team in production.

**Failed orders belong in the mart.** Failed attempts reveal friction points and help evaluate channel quality. Filtering them out is an opinionated data loss — it's easier for stakeholders to exclude them (`where not order_is_failed`) than to recover them from an upstream model they don't know exists.

**The dataset is a bounded sample.** All ~1,000 customers were acquired in 2023, with no new acquisition after December 2023. This means new-vs-repeat revenue splits are misleading after that point, cohort analysis is limited to 12 monthly cohorts, and recency metrics are anchored to the dataset's end date (~August 2024). The models are designed for production where acquisition is ongoing; the sample just limits which analyses produce meaningful results.

**Exchange rates are static but modeled for change.** The provided rates are a point-in-time snapshot. I built a snapshot (`snp_raw__exchange_rates`) with SCD Type 2 tracking and a `convert_to_usd` macro that supports temporal conversion. With the current data both modes produce identical results, but the design is ready for when rates change.

---

## 3. Data quality issues

**Multiple rows per order — by design, not a defect.** The source `orders` table contains one row per status change (e.g. `completed → refunded`). I preserve the full history in staging and resolve to the latest status in `int_orders_latest` using `row_number() over (partition by order_id order by updated_at desc)`. The `unique` test on `order_id` is applied only after deduplication.

**Orphaned `user_id = "None"`.** One order has `user_id` set to the literal string `"None"` rather than a valid ID or SQL null. This passes `not_null` tests — it's only caught by the `relationships` test between `orders.user_id` and `stg_raw__users.user_id`. The test is configured with `severity: warn` so it flags the issue without blocking runs. The order is preserved in the mart with null user dimensions (via the left join). In production, this would be reported to the source team.

**No null `status` or `created_at` values found.** These columns could theoretically be null for in-progress records. The current dataset has none, so I removed source-level `not_null` tests that would fire on inherent source behavior rather than fixable bugs. If nulls appear in future loads, the staging model would need a filter or coalesce strategy.

**"Unlimited" package is ambiguous.** Unlimited plans have no data allowance or validity period in the `esim_package` string — both `package_data_gigabytes` and `package_validity_days` are null. The source data doesn't tell us whether "Unlimited" means unlimited data, unlimited validity, or both. I flag these with `is_unlimited_package = true` and leave the parsed fields null rather than guessing. In production, this would need clarification from the product team.

**eSIM package format assumed stable.** The regex parsing (`(\d+)GB`, `(\d+) Days`) uses `cast` rather than `safe_cast` so unexpected format changes fail loudly instead of silently producing nulls.

---

## 4. Data cleaning vs business logic

The dividing line: if it requires a business decision, it's not cleaning.

**Staging handles mechanical cleanup** — type casting, `trim()`, `upper()` on ISO codes, `lower(replace(x, ' ', '_'))` on payment methods, regex parsing of `esim_package`. These are deterministic transformations with no judgment calls. The original values are either preserved alongside (e.g. `esim_package` kept next to the parsed columns) or the transformation is reversible.

**Intermediate handles business rules** — "most recent status wins" is a business decision about which row represents the truth. So is "join users to orders" and "convert amounts to USD." These transformations change the grain or meaning of the data, so they live in a separate layer where the logic is explicit and testable.

**Marts handle analytical logic** — customer sequencing (what counts as a purchase? are refunds included?), derived metrics (`days_since_first_purchase`, `is_repeat_purchaser`), and denormalization for stakeholder convenience. This is where business questions are pre-answered so stakeholders don't have to re-derive them.

A concrete example: `payment_method` normalization (`Apple Pay` → `apple_pay`) is cleaning — it standardizes format without changing meaning. Categorizing `apple_pay` and `google_pay` into `digital_wallet` would be business logic — it requires a decision about grouping that stakeholders might disagree with. The first belongs in staging; the second would belong in intermediate or marts, and ideally in a seed-driven mapping table rather than hardcoded SQL.
