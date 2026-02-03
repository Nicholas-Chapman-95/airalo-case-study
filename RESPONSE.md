# Written Response

---

## 1. How I approached the data modeling problem

I started by exploring the raw data to understand primary keys, uniqueness, and grain. The most important discovery was that `raw.orders` contains multiple rows per `order_id` — each status change (e.g. completed → refunded) creates a new row. This meant deduplication had to be a first-class concern, not an afterthought, and it shaped the entire intermediate layer design.

From there I followed dbt's three-layer architecture:

1. **Staging** — one model per source table, 1:1. Light cleaning only: renaming, casting, normalizing inconsistent casing. No joins, no business logic. I establish the grain and primary key at this layer — for orders that's `order_status_key` (a surrogate key per status-change row), for users it's `user_id`.
2. **Intermediate** — reusable building blocks that do the heavy lifting. Deduplication (`int_orders_latest_status`), enrichment with currency conversion and country names (`int_orders_enriched`), purchase sequencing (`int_orders_numbered_per_customer`), and customer-level aggregation (`int_orders_aggregated_to_customers`). Each intermediate model has a single, distinct responsibility — this makes them independently testable and reusable.
3. **Marts** — the published BI interface. `orders` is a thin view that provides a stable column contract over the sequenced data. `customers` is an incremental merge that rolls up order activity to one row per customer. Both marts include all order statuses (completed, refunded, failed) so downstream consumers choose their own filters rather than having data pre-filtered away.

Pre-computing derived fields like `customer_purchase_number`, `days_since_previous_purchase`, and `is_first_purchase` in the intermediate layer means the window functions run once at build time rather than on every dashboard query. In BigQuery this matters for cost and performance — a BI tool firing `row_number() over (partition by user_id ...)` on every page load burns slots and increases dashboard latency. Pre-computing these columns shifts that work to the scheduled dbt run, so dashboards read simple, pre-joined columns instead of recomputing window functions against the full table. It also makes the BI layer more usable: analysts and non-technical stakeholders can drag governed, tested columns into a dashboard without needing to understand the underlying SQL. Similarly, codifying metrics like `repeat_purchaser_rate` and `new_customer_rate` in the YAML configs ensures "repeat purchaser rate" means the same thing in every dashboard rather than being left to ad-hoc SQL interpretations.

The marts use two sequencing namespaces to serve different analytical needs from the same table: **attempts** (all orders including failures) for funnel analysis, and **purchases** (completed + refunded only) for lifecycle and retention. This avoids the compromise of either excluding failed orders (losing funnel data) or including them in purchase sequencing (inflating counts and skewing timing).

Two supporting pieces worth noting:

- **`seed_countries`** — a CSV seed mapping ISO country codes to human-readable names. This is small, stable reference data that doesn't come from the source system and changes rarely (country names don't shift often). A seed is the right fit over a source table (no upstream system owns this data) or hardcoded SQL (a `case when` with 200+ countries would be unmaintainable). It lands in the `raw` schema alongside source tables since it serves the same role — base lookup data.
- **`convert_to_usd` macro** — centralizes currency conversion logic so the formula, rate source, and temporal join are defined once. Even though only orders uses it today, in a production Airalo codebase this logic would appear across multiple marts (orders, refunds, payouts, etc.). The macro queries the exchange rate snapshot and supports both current-rate and point-in-time conversion, so switching to historical rates when the snapshot accumulates enough history is a one-line change. A `generate_schema_name` macro override is also included so that `+schema: staging` produces a BigQuery dataset called `staging` rather than dbt's default `analytics_staging`.

The incremental models use **merge** strategy rather than `insert_overwrite` or `microbatch`. This is driven by a mismatch between the incremental filter and the partition key: the filter is on `updated_at` (when the status changed) but the tables are partitioned by `created_at` / `signup_at` (when the order was placed or the user signed up). A status change today can affect an order in any historical partition. `insert_overwrite` and `microbatch` replace entire partitions — so processing today's batch would drop and rebuild a partition from, say, three months ago, destroying sibling rows that weren't part of the current batch. `merge` does row-level upserts keyed on `order_id` or `user_id`, so only the rows that actually changed are updated, leaving the rest of the partition intact.

---

## 2. Key assumptions

- **Production data volume would be significantly larger.** The sample covers ~1,000 users and ~3,000 orders, which could easily be materialized as full tables on every run. But in a production environment with millions of rows, that approach doesn't scale. I designed the incremental models (`int_orders_latest_status`, `int_orders_enriched`, `customers`) to process only changed data within a batch window, assuming an orchestrator like Airflow would pass `data_interval_start`/`data_interval_end` on a daily schedule. The `schedules.yml` file documents how these DAGs would be structured.
- **The users table is append-only.** There's no `updated_at` column on `raw.users`, so I treat user attributes (platform, acquisition channel, IP country) as signup-time metadata that doesn't change. If the source system does silently overwrite these fields, tracking changes would require adding a dbt snapshot on `raw.users`.
- **Refunded orders are purchases.** A refund means the customer signed up, selected a plan, and completed payment — the refund is a post-purchase event. Excluding refunds from the purchase sequence would undercount engagement and distort retention metrics. Revenue analysis can filter on `order_is_completed`.
- **Most recent status wins.** The source contains multiple rows per order (one per status change). I assume the row with the latest `updated_at` represents the current state. This is standard for append-only event patterns but would need validation with the source team in production.
- **Failed orders belong in the mart.** Failed attempts reveal friction points and help evaluate channel quality. Filtering them out is opinionated data loss — it's easier for stakeholders to exclude them (`where not order_is_failed`) than to recover them from an upstream model they don't know exists.
- **Exchange rates would change constantly in production.** The sample data has a single static rate per currency, but in a real environment rates fluctuate daily. I built `snp_raw__exchange_rates` as a dbt snapshot using the `check` strategy, which implements SCD Type 2 tracking — every time a rate changes, the old row gets a `dbt_valid_to` timestamp and a new row is inserted. This builds a history of rates with validity windows automatically. With the current static data the snapshot has no effect, but the design is production-ready for when rates start moving.
- **Marts are designed to feed a BI tool like Lightdash.** Column-level `meta.dimension` and `meta.metrics` blocks in the mart YAML configs define governed metric definitions that a BI layer picks up automatically — the specifics of why this matters for cost and consistency are covered in section 1.
- **The dataset is a bounded sample.** All ~1,000 customers were acquired in 2023, with no new acquisition after December 2023. The models are designed for production where acquisition is ongoing; the sample just limits which analyses produce meaningful results (e.g. new-vs-repeat revenue splits are misleading after the acquisition period ends).

---

## 3. Data quality issues and how I handled them

### At the staging layer (format-level fixes that don't change meaning)

- **Inconsistent casing** — the source has mixed case across tables (`iOS`/`android`, `Apple Pay`/`apple_pay`, `Referral Link`/`organic`). Staging normalizes all strings: `lower(trim())` for categorical fields, `upper(trim())` for ISO country codes to match the standard convention.
- **Payment methods with spaces** — `Apple Pay`, `Google Pay` etc. are normalized to `snake_case` (`apple_pay`, `google_pay`) so downstream filters don't need to handle variations.
- **eSIM package string parsing** — the raw `esim_package` field is a human-readable label like `"3GB - 30 Days"` or `"Unlimited"`. Staging extracts `package_data_gigabytes` and `package_validity_days` via regex so downstream models don't repeat the parsing. Uses `cast` (not `safe_cast`) so unexpected format changes fail loudly instead of silently producing nulls. Note: `"Unlimited"` only tells us the data allowance is unlimited — it doesn't include a plan validity period like the other packages do (e.g. `"7 Days"`, `"30 Days"`). Both parsed fields are null for these plans. Ideally the source would have a separate `plan_validity_days` field rather than encoding it in a label string; this is flagged with `is_unlimited_package = true` so downstream consumers can handle it explicitly.
- **Floating-point precision** — order amounts and exchange rates are cast to `numeric` (not `float64`) to avoid rounding errors in currency calculations (e.g. `0.1 + 0.2 != 0.3` with floats).
- **Type casting** — IDs cast to `string` for join safety and surrogate key compatibility, timestamps explicitly cast from source strings for type safety in date logic.
- **Status booleans** — `is_completed`, `is_refunded`, `is_failed` flags derived in staging so downstream models don't repeat `where status = 'completed'` everywhere. Also convenient for `sum()` aggregations (BigQuery casts `true` as 1).
- **Null `ip_country`** — 39 of 1,000 users (3.9%) have a null `ip_country`. The nulls are distributed evenly across all platforms, acquisition channels, and signup months — no concentration in any segment or time period. These users still place orders normally (destination country is always populated on their orders). The even distribution suggests IP geolocation lookup failures at signup time, likely due to VPN/proxy usage that the geolocation service couldn't resolve — plausible for a travel eSIM user base. The column is left as null rather than imputed, and downstream models handle it gracefully via left joins. In production, this would be worth monitoring for trend changes.
- **Null `card_country`** — ~9.6% of orders have a null `card_country`. This is spread roughly evenly across all payment methods (credit card 8%, airmoney 10%, paypal 11%) — not concentrated on cardless methods. This indicates the payment processor sometimes doesn't return card country regardless of payment type, rather than it being a function of the payment method. The column is left as null with no imputation.

### At the intermediate layer (structural and relational issues)

- **Multi-row orders** — the biggest issue. `raw.orders` has multiple rows per `order_id` due to status changes over time. `int_orders_latest_status` deduplicates using `row_number() over (partition by order_id order by updated_at desc)` to keep only the most recent status per order. The `unique` test on `order_id` is only applied after deduplication — it would be wrong to test it at the staging level where duplicates are expected.
- **Orphaned user ID (`"None"` string)** — one order has `user_id` set to the literal string `"None"` rather than SQL null — a source system bug where Python `None` was serialized as a string. Staging converts this to actual `NULL` via `nullif(cast(user_id as string), 'None')` — this is data cleaning (fixing a broken representation), not a business decision about whether to keep the order. The order is preserved in `int_orders_enriched` via a left join with null user dimensions. A `warn`-severity relationship test on the `orders` mart flags the source issue without blocking the pipeline. In production, this would be reported to the source team for a fix.

### Testing strategy

**Generic tests** enforce data contracts at each layer. Primary key integrity (`unique` + `not_null`) is tested on every model's grain column — `order_id` on order-grain models, `user_id` on customer-grain models. Referential integrity is tested with `relationships` tests at `warn` severity so a single orphaned foreign key doesn't block the pipeline (the `"None"` user ID issue above is a real example). Beyond structural tests, `expression_is_true` tests guard derived numeric columns against logic bugs: `order_amount_usd >= 0`, `days_to_second_purchase >= 0`, `customer_lifetime_days >= 0`, and `total_orders >= 0`. Boolean flags produced by `coalesce` (`is_repeat_purchaser`, `has_repeat_within_90d`, `has_repeat_within_365d`) have `not_null` tests to confirm the coalesce is actually preventing nulls from leaking through.

**Unit tests** validate the business logic that generic tests can't reach — they test *correctness*, not just *plausibility*. Four unit tests cover the critical transformations:

- **Dedup logic** (`int_orders_latest_status`) — given orders with multiple status rows, only the latest row is kept. Tests single-row, two-row, and three-row cases to confirm `row_number()` ordering works correctly.
- **USD conversion** (`int_orders_enriched`) — verifies the exchange rate join and `amount / nullif(rate, 0)` arithmetic produces correct results (e.g. EUR 100 at rate 0.92 → USD 108.70).
- **Purchase sequencing** (`int_orders_numbered_per_customer`) — verifies that attempt numbering includes all statuses, purchase numbering skips failed orders (null purchase number), and `days_since_previous_purchase` is computed between purchases only.
- **Customer derived columns** (`customers`) — verifies repeat purchaser flags, lifetime days, and repeat window booleans for both a multi-purchase customer and a single-purchase customer.

Each unit test overrides `is_incremental` to `false` so the full-refresh code path is exercised with controlled mock data, independent of warehouse state.

---

## 4. Data cleaning vs business logic: where each belongs

The principle is straightforward: **staging stays 1:1 with the source table and never joins other tables**. If an operation only needs the source column to fix its representation (casing, trimming, type casting, regex parsing), it belongs in staging. If it needs data from another source to derive a new value, or it requires a business decision about which rows to keep or how to reshape the grain, it belongs in intermediate.

Concrete examples from this project:

| Operation | Layer | Why |
|---|---|---|
| `lower(trim(platform))` | Staging | Normalizing the source value — no external data needed |
| `upper(trim(card_country))` | Staging | Standardizing ISO codes to their conventional format |
| `regexp_extract(esim_package, ...)` | Staging | Parsing a structured string into typed components |
| `cast(amount as numeric)` | Staging | Fixing the source type for precision |
| `nullif(user_id, 'None')` | Staging | Fixing a serialization bug — converting a stringified Python `None` to SQL `NULL` |
| Deduplicating multi-row orders | Intermediate | Applies a business rule ("most recent status wins") to reshape the grain |
| `amount / nullif(usd_rate, 0)` | Intermediate | Requires a join to `exchange_rates` — a different source |
| Country code → name lookup | Intermediate | Requires a join to the `seed_countries` file |
| Purchase sequencing (window functions) | Intermediate | Business logic that spans multiple orders per customer |
| `is_repeat_purchaser`, `days_to_second_purchase` | Marts | Customer-grain derivations that aggregate across the full order history |

A concrete example of the boundary: `payment_method` normalization (`Apple Pay` → `apple_pay`) is cleaning — it standardizes format without changing meaning. Categorizing `apple_pay` and `google_pay` into `digital_wallet` would be business logic — it requires a decision about grouping that stakeholders might disagree with. The first belongs in staging; the second would belong in intermediate or marts, ideally driven by a seed-based mapping table rather than hardcoded SQL.

This separation keeps staging models simple and auditable (you can always compare them directly to the source), while intermediate models are where the data gets shaped into something analytically useful. If the cleaning vs logic decision is ambiguous, the question to ask is: "would two reasonable people disagree about how to do this?" If yes, it's business logic, not cleaning.
