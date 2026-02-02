# Architecture Decisions

Reference log for design choices made during this case study.

---

## Project foundations

### Three-layer model architecture

The project uses a staging → intermediate → marts pattern:

- **Staging** cleans and type-casts raw source data. No business logic.
- **Intermediate** applies business logic that transforms grain or resolves data quirks
  (e.g. deduplication). Keeps marts simple.
- **Marts** serve final, query-ready tables to stakeholders.

This separation means each layer has a single responsibility and changes in one layer
don't cascade unpredictably.

### Materialization strategy

| Layer        | Materialization | Why |
|--------------|-----------------|-----|
| Staging      | view            | No storage cost; always reflects latest source data; fast iteration |
| Intermediate | view            | Same rationale — intermediate logic is lightweight enough for views |
| Marts        | table           | Optimized for downstream queries; stakeholders hit pre-computed tables |

Configured in `dbt_project.yml` at the directory level so individual models don't need
`{{ config() }}` blocks unless they need to override (e.g. for partitioning/clustering).

### BigQuery as the warehouse

The project targets BigQuery (`dbt-bigquery >= 1.11.0`). This influences dialect choices:
`cast(x as string)` instead of `::text`, `cast(x as numeric)` instead of `::decimal`,
`timestamp` type semantics, and partition/clustering configuration on tables.

### Minimal dependencies

Only three Python packages: `dbt-bigquery`, `sqlfluff`, `sqlfluff-templater-dbt`. No
`dbt-utils`, `dbt-expectations`, or other third-party dbt packages. This keeps the project
lightweight and avoids version conflicts. Custom macros can be added as needed, but we
avoid pulling in large packages for one or two functions.

### Python 3.13 with uv

Pinned to Python 3.13 (`.python-version`) and managed with `uv` instead of pip. uv provides
faster dependency resolution and reproducible lockfiles. The `pyproject.toml` is the single
source of truth for dependencies.

---

## SQL style & linting

### sqlfluff configuration

Key choices in `.sqlfluff`:

- **80-character line limit** — forces readable, well-structured queries. Long lines usually
  mean the SQL needs restructuring.
- **Lowercase everything** — keywords (`select`, `from`), identifiers, functions, types, and
  literals are all lowercase. This follows the dbt style guide. The traditional convention of
  uppercase keywords (`SELECT`, `FROM`) exists to visually distinguish keywords from
  identifiers, but modern editors with syntax highlighting make that unnecessary — colors do
  the job. Lowercase is easier to type, easier to read, and removes one more style decision.
- **Explicit aliases required** — table and column aliases must use `as`. Prevents ambiguity
  between column names and implicit aliases.
- **Trailing commas forbidden on last column** — cleaner diffs when adding columns (new line
  adds both comma and column to the previous line, but that's a smaller diff than trailing
  comma style in practice with this team's workflow).
- **4-space indentation** — consistent with Python convention in the same repo.
- **Jinja templater** — recognizes `{{ source() }}`, `{{ ref() }}`, and other dbt macros.

---

## Source layer

### Single source named `raw`

All three tables (`orders`, `users`, `exchange_rates`) live in the same BigQuery dataset
(`airalo-486008.raw`), so they share one source definition. If tables from a different system
are added later, they get their own source name and subdirectory.

### Source freshness: warn at 36h, error at 72h

- `warn_after: 36 hours` — alerts on slow loads before they become critical.
- `error_after: 72 hours` — fails the freshness check if data is severely stale, which can
  gate downstream runs.
- `exchange_rates` opts out entirely (`loaded_at_field: null`) because it's a lookup table
  with no time-based loading pattern.

### File naming conventions

**Leading underscore on YAML files.** All `.yml` files are prefixed with `_` (e.g.
`_orders.yml`, `_raw__sources.yml`). This is the official dbt convention — the underscore
sorts YAML files to the top of every directory listing, visually separating configuration
from SQL model files. Without it, YAML files scatter alphabetically among `.sql` files and
are harder to find at a glance.

**Double underscore (`__`) as namespace separator.** The `__` delimiter appears in staging
model names (`stg_raw__orders`) and source YAML files (`_raw__sources.yml`). It separates
the source system name from the entity or file type. A single underscore would be ambiguous
when names themselves contain underscores (e.g. `stg_jaffle_shop_orders` — is the source
`jaffle_shop` or `jaffle`?). At the intermediate layer, the double underscore is dropped
because models operate on business-conformed concepts, not source-system data (see
"Naming convention" in the Intermediate section).

**Per-model YAML files over per-folder.** The official dbt Labs recommendation is per-folder
(`_[dir]__models.yml` grouping all models in one file). This project uses per-model instead
(e.g. `_stg_raw__orders.yml` alongside `stg_raw__orders.sql`). This is an accepted
alternative (Brooklyn Data Co. convention) chosen because:

- Diffs are isolated to the model that changed — no unrelated models in the same diff.
- Merge conflicts are less likely when multiple people work on different models.
- It's immediately obvious which YAML belongs to which model.
- Missing YAML files are visible — an untested model has no `.yml` file.

The only shared YAML is `_raw__sources.yml` since sources are inherently a group definition.

**SQL model naming by layer:**

| Layer        | Pattern                        | Example                          |
|--------------|--------------------------------|----------------------------------|
| Staging      | `stg_[source]__[entity]s.sql`  | `stg_raw__orders.sql`            |
| Intermediate | `int_[entity]s_[verb]ed.sql`   | `int_orders_latest.sql`          |
| Marts        | `[entity]s.sql` (plain English)| `orders.sql`                     |

The progression reflects how data moves from source-system-specific (double underscore
namespaced) toward business-conformed concepts (plain English, no prefixes).

### Source tests only for upstream-fixable issues

Source-level tests should only flag problems that can be corrected in the source system.
Tests that fire on inherent source behavior create noise and erode trust in the test suite.

**Kept:**
- `unique` + `not_null` on `users.user_id` — duplicate users can be deleted at source
- `unique` + `not_null` on `exchange_rates.currency` — duplicate rates are a source bug
- `not_null` on `orders.order_id` and `orders.user_id` — null PKs/FKs are source bugs

**Removed:**
- `not_null` on `orders.status` and `orders.created_at` — if these are null, it may be due
  to how the source system handles in-progress records. Not fixable upstream; mitigate in
  staging instead (e.g. `coalesce`, `where` filter).

---

## Staging layer

### File naming: `stg_raw__[entity].sql`

Convention is `stg_[source]__[entity]s`. The double underscore separates source name from
entity name, which matters when multiple sources exist (e.g. `stg_stripe__payments.sql` vs
`stg_shopify__orders.sql`). Setting the pattern early avoids a painful rename later.

### CTE structure

Every staging model follows the same pattern:

```sql
with source as (
    select ... from {{ source('raw', 'table') }}
)
select ... from source
```

The `source` CTE isolates the raw data pull. The final `select` applies all transformations.
This makes it easy to see what comes from the source vs what the model changes.

### Primary keys cast to string

Both `order_id` and `user_id` are numeric integers in the source. We cast them to `string`
in staging because:

- **Join safety** — avoids implicit type coercion when joining across models that may have
  different ID types (e.g. an integer FK joining to a string PK silently casts and can skip
  index usage in some warehouses).
- **Surrogate key generation** — `dbt_utils.generate_surrogate_key()` expects string inputs.
  Casting upstream means downstream models don't each need to handle it.
- **Consistency** — every ID column across the project is a string. No guessing.

### `cast` over `safe_cast`

We use `cast` (not `safe_cast`) in staging. `safe_cast` returns `null` instead of erroring
on bad data, which sounds safer but actually hides data quality problems. If an `order_id`
can't cast to string or an `amount` can't cast to numeric, we want that to fail loudly — it
means something unexpected is in the source data.

`safe_cast` is appropriate for messy, user-entered text fields where bad values are expected
and you want to gracefully degrade (e.g. casting a freeform "age" field where someone typed
"N/A"). That doesn't apply here — our source columns have well-defined types.

Rule of thumb: `cast` by default in staging (fail fast). Only use `safe_cast` when you
expect bad values and have a deliberate strategy for the resulting nulls.

### Amounts cast to `numeric`, not `float64`

Financial values (`amount`, `usd_rate`) use `numeric` (BigQuery's `NUMERIC` / `BIGNUMERIC`)
to avoid floating-point precision errors. `float64` would introduce rounding issues on
currency calculations (e.g. `0.1 + 0.2 != 0.3`).

### Column ordering by type

Columns in staging models are grouped: IDs first, then strings, numerics, booleans (when
present), and timestamps last. This makes it easy to scan a model and find what you need
without reading every line.

### `esim_package` parsed into components

The raw `esim_package` column contains values like `1GB - 7 Days`, `10GB - 30 Days`, and
`Unlimited`. We parse this into three derived columns in staging:

- `package_data_gb` (int64) — extracted via `regexp_extract(esim_package, r'(\d+)GB')`.
  Null for Unlimited plans.
- `package_validity_days` (int64) — extracted via `regexp_extract(esim_package, r'(\d+) Days')`.
  Null for Unlimited plans.
- `is_unlimited_package` (boolean) — `esim_package = 'Unlimited'`.

The original `esim_package` column is preserved. Parsing here follows the DRY principle —
every downstream model that needs GB or days would otherwise repeat the regex. Uses `cast`
(not `safe_cast`) so unexpected format changes fail loudly.

### `payment_method` normalized to snake_case

Source values are mixed case with spaces (`Apple Pay`, `Credit Card`). We apply
`lower(trim(replace(payment_method, ' ', '_')))` to produce `apple_pay`, `credit_card`, etc.
This is a basic string cleanup — consistent with the project's lowercase convention and
makes downstream `group by` / `where` filters simpler.

Categorization (e.g. grouping `apple_pay` and `google_pay` into `digital_wallet`) is not
done here because it would require a seed join → belongs in intermediate or marts.

### Status booleans

Three boolean flags derived from `status`: `is_completed`, `is_refunded`, `is_failed`.
These are the most common downstream filters. Booleans are more readable than
`where status = 'completed'` scattered across multiple models, and they make `sum()` easy
(BigQuery casts `true` as 1 in aggregations).

A numerical status ordering (1=pending, 2=completed, etc.) was considered and rejected —
it embeds business logic about ordering that could change and requires downstream consumers
to know the mapping.

### `platform`, `acquisition_channel`, and `ip_country` normalization

`platform` and `acquisition_channel` are lowercased and trimmed; `acquisition_channel` also
replaces spaces with underscores (same pattern as `payment_method` in orders). `ip_country`
is uppercased and trimmed to match ISO 3166-1 alpha-2 convention and the `seed_countries`
lookup.

As of the initial data load, all three fields are already clean — no leading/trailing
whitespace, no mixed casing within a field. The `trim()` and `upper()` on `ip_country` are
purely defensive. We keep them because they're zero-cost in a view and protect against
future data quality drift from upstream.

### ISO codes: `upper(trim())` consistently

`currency`, `card_country`, and `destination_country` in orders all receive `upper(trim())`
— matching the same treatment applied to `ip_country` in users. ISO 4217 (currency) and
ISO 3166-1 alpha-2 (country) codes are uppercase by convention, and applying `upper()`
defensively ensures consistency across the project even if source data drifts.

`card_country` has expected nulls (e.g. Airmoney payments have no card). `upper(null)`
returns null, so the defensive normalization is safe for nullable columns.

### No macros for staging transformations

None of the staging transformations warrant a macro:

- esim parsing is domain-specific to one column in one model.
- `lower(trim(replace(x, ' ', '_')))` is a one-liner used once.
- Status booleans are trivial comparisons.

The rule: don't create abstractions until a pattern appears in 3+ places. Premature macros
add indirection without reuse benefit.

### No joins, aggregations, or deduplication in staging

Staging models clean individual source tables — one model per source table, no exceptions.
Joins combine data (intermediate/marts concern). Aggregations change grain (marts concern).
Deduplication is business logic about which row to keep (intermediate concern).

### `order_id` is NOT tested for uniqueness

`raw.orders` contains multiple rows per `order_id` (one per status change, e.g.
`completed → refunded`). This is by design, not a data quality issue. The `unique` test is
intentionally omitted at both the source and staging level. Deduplication happens in
`int_orders_latest` using the most recent `updated_at` per `order_id`.

### Staging model tests in per-model YAML files

Separate from source tests. These validate the output of the staging models themselves:

- `stg_raw__orders` — `not_null` on `order_id` and `user_id` (no `unique` — multi-row)
- `stg_raw__users` — `unique` + `not_null` on `user_id`
- `stg_raw__exchange_rates` — `unique` + `not_null` on `currency`

---

## Intermediate layer

### Naming convention: `int_[entity]s_[verb]ed`

Intermediate model names include a past-tense action verb describing the transformation:

- `int_orders_latest` — describes the business outcome (each order's status is
  resolved to its most recent state) rather than the SQL technique (`deduplicated`).
- `int_orders_customer_sequenced` — describes what was added (customer-level sequencing).

The single underscore between entity and verb distinguishes intermediate models from staging
models, which use a double underscore to separate source system from entity name
(`stg_raw__orders`). At the intermediate layer, models operate on business-conformed concepts
rather than source-system data, so the double underscore is dropped.

The verb requirement follows dbt best practices and distinguishes intermediate models (which
*do* something to data) from staging models (which just clean) and marts (which just serve).

### Deduplication in intermediate, not staging

`int_orders_latest` takes the most recent row per `order_id` (by `updated_at`). This
belongs in intermediate because:

- It's business logic — "most recent status wins" is a business rule, not a data cleaning
  operation.
- Staging should preserve the source grain. If someone needs the full status history later,
  they can query `stg_raw__orders` directly.
- It keeps staging models simple and predictable.

### `row_number()` over `qualify`

The deduplication uses a `row_number()` window function in a CTE with `where row_num = 1`,
rather than BigQuery's `QUALIFY` clause. Reasons:

- More portable across warehouses if the project ever migrates.
- Easier to debug — you can inspect the `orders_ranked_by_recency` CTE and see all rows with their row numbers.
- More familiar to analysts who may not know `QUALIFY`.

### Intermediate materialized as view

The deduplication is lightweight enough that a view works fine. If the orders table grows
large and query performance suffers, this can be changed to `table` or `incremental` in
`dbt_project.yml` without touching the SQL.

---

## Macros

### `convert_to_usd` — currency conversion with temporal awareness

A reusable macro that converts a monetary amount to USD using exchange rates. Accepts an
amount column, a currency column, and an optional timestamp column.

**Why a macro?** Currency conversion is a cross-cutting concern — any model dealing with
revenue, LTV, or cost analysis needs amounts in a common currency. Even though only
`orders` uses it today, in a production Airalo codebase this logic would appear in
multiple marts (orders, refunds, payouts, etc.). Centralizing it means the conversion formula,
rate source, and temporal join logic are defined once.

**Why a snapshot?** Exchange rates change over time. A static lookup table only gives you
today's rate, which means historical orders get retroactively re-valued — a $100 order from
six months ago would be converted at today's rate, not the rate on the day of purchase. The
`snp_raw__exchange_rates` snapshot implements SCD Type 2 via dbt's `check` strategy:

- Each time `dbt snapshot` runs, it compares the current `usd_rate` for each currency against
  the previously stored value.
- If a rate changed, the old row gets a `dbt_valid_to` timestamp and a new row is inserted
  with `dbt_valid_to = null`.
- This builds a history of rates with validity windows automatically.

**Temporal vs current conversion:**

```sql
-- Point-in-time: uses the rate valid when the order was created
{{ convert_to_usd('amount', 'currency', 'created_at') }} as amount_usd

-- Current: uses the latest rate (dbt_valid_to is null)
{{ convert_to_usd('amount', 'currency') }} as amount_usd
```

With the current static CSV data, both modes produce identical results — every rate has a
single snapshot record with `dbt_valid_to = null`. But the design is production-ready for when
rates start changing, and it demonstrates awareness of temporal data challenges in financial
reporting.

**`check` strategy over `timestamp`:** The exchange rates source has no `updated_at` column,
so the `timestamp` strategy isn't available. The `check` strategy compares column values
directly, which is appropriate for small lookup tables.

**Current limitation: non-temporal conversion in `orders`.** The macro supports
point-in-time conversion (`convert_to_usd('amount', 'currency', 'created_at')`), but
`orders` currently uses the non-temporal mode (`convert_to_usd('amount', 'currency')`).
This is because `dbt snapshot` sets `dbt_valid_from` to the timestamp when the snapshot was
first run — which is after all historical orders were created. The temporal `where` clause
(`created_at >= dbt_valid_from`) therefore matches no rows, producing null `amount_usd` for
every order.

Once the snapshot has been running long enough to accumulate rate history covering the order
date range (or if historical rates are backfilled into the snapshot table), `orders` can
be switched to the temporal call. With the current static CSV data both modes produce
identical results anyway — every currency has exactly one rate.

---

## Marts layer

### `orders` — plain English name, single wide denormalized table

The mart is named `orders`, not `fct_orders`. dbt best practices call for plain English
entity names in the marts layer — no `fct_` or `dim_` prefixes. The model lives in the
`marts/` directory, which already communicates its role in the DAG.

The mart is a single wide table rather than a star schema with separate fact and dimension
tables. This follows modern dbt practice: the mart should be analytics-ready without
requiring consumers to write joins. User attributes are denormalized directly onto each
order row.

In a larger codebase with many marts sharing user dimensions, a separate `dim_users` would
be justified to avoid repeating the user join logic. For this project's scope, the
denormalized approach is simpler and produces a better stakeholder experience — one table
answers all the business questions.

### All order statuses included

`orders` contains completed, refunded, **and** failed orders. Failed orders are not
filtered out because:

- Marketing needs funnel visibility — failed attempts reveal friction points and help
  evaluate channel quality (a channel with high attempt-to-failure rates needs attention).
- Filtering in the mart is an opinionated data loss. It's easier for stakeholders to filter
  out failed orders (`where is_completed`) than to recover them from an upstream model they
  may not know exists.

### Two-namespace column design: attempts vs purchases

The mart uses two sets of sequencing columns to serve different analytical needs from the
same table:

**Attempt-level** (`customer_attempt_number`, `is_first_attempt`) — populated for every row,
including failed orders. Sequences all orders chronologically per customer. Useful for
funnel analysis and marketing attribution.

**Purchase-level** (`customer_purchase_number`, `is_first_purchase`, `previous_purchase_at`,
`days_since_previous_purchase`, `customer_first_purchase_at`, `days_since_first_purchase`) —
populated only for completed and refunded orders. Null for failed orders. Useful for
customer lifecycle, retention, and revenue analysis.

This design avoids the compromise of either excluding failed orders (losing funnel data) or
including them in purchase sequencing (inflating purchase counts and skewing inter-purchase
timing). Window functions cannot conditionally skip rows, so purchase-level metrics are
computed on a filtered subset in `int_orders_customer_sequenced` and left-joined back,
giving failed orders null values naturally.

### Refunded orders count as purchases

A refunded order still represents a customer who engaged with the product — they signed up,
selected a plan, and completed payment. The refund is a post-purchase event. Excluding
refunds from the purchase sequence would undercount customer engagement and distort
retention metrics.

For revenue analysis, stakeholders can filter on `is_completed` to exclude refunded amounts.
The `is_refunded` boolean makes this straightforward.

### `amount_usd` uses point-in-time conversion

The `convert_to_usd` macro is called with the order's `created_at` timestamp, so each order
is converted at the exchange rate that was valid when it was placed. This prevents historical
revenue figures from shifting when exchange rates change — a completed order from three
months ago always reports the same USD amount.

### Partitioning and clustering on `orders`

The `orders` mart is partitioned and clustered in BigQuery:

- **Partition by `created_at` (day granularity)** — most analytical queries on orders filter
  by date range (weekly revenue, monthly cohorts, trending). Day-level partitioning lets
  BigQuery prune irrelevant partitions and scan only the relevant date range. This reduces
  both query cost and latency on large tables.
- **Cluster by `status`, `destination_country`** — the two most common filter and group-by
  columns after date. `status` appears in virtually every query (`where is_completed` or
  `where status = 'completed'`). `destination_country` supports market-level analysis.
  Clustering sorts data within each partition by these columns, improving scan efficiency
  when they appear in `WHERE` or `GROUP BY`.

Staging and intermediate models are materialized as views, so partitioning and clustering
do not apply. The snapshot table (`snp_raw__exchange_rates`) is too small to benefit.

In the current dataset (~2,700 rows), partitioning and clustering have negligible impact.
The config is added to demonstrate production-readiness — when the table grows to millions
of rows, these settings avoid a retroactive migration.

### Customer sequencing in intermediate, not the mart

The window functions for customer sequencing live in `int_orders_customer_sequenced` rather
than directly in `orders`. This keeps the mart focused on joins and column selection,
while the intermediate model owns the business logic of "what counts as a purchase" and "how
to sequence orders". If the sequencing rules change (e.g. excluding refunds from the
sequence), only the intermediate model needs updating.

---

## Seeds

### `seed_countries` in the `raw` schema

Seeds are configured with `+schema: raw` in `dbt_project.yml` so they land alongside the
source data rather than in the default target schema (`analytics`). Seeds are reference data
that serves the same role as source tables — lookup tables loaded into the warehouse. Placing
them in `raw` keeps all base data in one schema and avoids polluting the `analytics` schema
with tables that aren't analytics-ready outputs.

---

## Data quality issues

### Duplicate order rows (by design, not a defect)

`raw.orders` contains multiple rows per `order_id` — one for each status change (e.g.
`completed → refunded`). This is an append-only event pattern, not dirty data. We preserve
the full history in `stg_raw__orders` and resolve to the latest status per order in
`int_orders_latest`. The `unique` test on `order_id` is intentionally omitted at
the staging level and applied only after deduplication.

### Orphaned `user_id = "None"` (source data issue)

One order in the source data has `user_id` set to the literal string `"None"` rather than a
valid user ID or SQL null. This was discovered by the `relationships` test on the `orders`
mart, which checks that every `user_id` exists in `stg_raw__users`.

Because `"None"` is a non-null string, it passes `not_null` tests — the issue is only
visible through referential integrity checks. The `relationships` test is configured with
`severity: warn` so it flags the issue without blocking pipeline runs.

In a production system, the fix would be to:

1. Report the issue to the source system team for correction.
2. Optionally add a staging filter (`where user_id != 'None'`) or coalesce to null if the
   business decides orphaned orders should be excluded. For now, the order is preserved in
   the mart with its user dimension columns set to null (via the `left join`).

### No null `status` or `created_at` values found

The source tests for `not_null` on `orders.status` and `orders.created_at` were removed
during initial development because these columns could theoretically be null for in-progress
records. In practice, the current dataset contains no nulls in either column. If nulls appear
in future loads, the staging model would need a `coalesce` or `where` filter strategy.

### Bounded dataset — all customers acquired in 2023

The dataset contains ~1,000 customers who were all acquired (made their first purchase) during
2023, with ~76–96 new customer orders per month. By January 2024, no new customers remain —
every order from 2024 onward is a repeat purchase by an existing customer.

This affects analysis in several ways:

- **New vs repeat revenue splits** are misleading after Dec 2023. The trend showing repeat
  revenue climbing from 11% to 100% reflects the dataset boundary, not a strategic shift
  away from acquisition. Charts using `is_first_purchase` to segment new vs repeat should
  be filtered to the active acquisition period (Jan–Dec 2023) for accurate interpretation.
- **Cohort analysis** is limited to ~12 monthly cohorts (Jan–Dec 2023). Standardized repeat
  rate trends and retention curves only have one year of acquisition cohorts to compare.
- **Recency metrics** like `days_since_last_order` are anchored to the dataset's end date
  (~Aug 2024), so customers who stopped buying in mid-2023 show ~12+ months of inactivity
  even though that may simply reflect the dataset ending.

This is a property of the sample dataset and would not apply in production where customer
acquisition is ongoing.

---

## Version control

### `.gitignore` strategy

Committed: source code, config, documentation.
Ignored: `target/` (compiled SQL), `dbt_packages/`, `logs/`, `.venv`, `.env`, `.DS_Store`.
Credentials and artifacts never enter the repo.
