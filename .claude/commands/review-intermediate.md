Review intermediate models in this dbt project against official dbt best practices.

## Rules to Check

### Purpose & Scope
- Intermediate models combine staging atoms into molecules — more intricate, connected shapes with specific purposes
- Appropriate use cases:
  1. Structural simplification — bringing together 4-6 entities/concepts to reduce mart complexity
  2. Re-graining — fanning out or collapsing to the correct composite grain
  3. Isolating complex operations — deduplication, complex window functions, pivots
- These models are NOT intended for output to dashboards or applications

### Structure & Organization
- Subdirectories should be organized by business grouping (NOT source system)
- Only create subdirectories if you have 10+ mart models
- File naming: `int_[entity]s_[verb]ed.sql` — must include action verb describing transformation
  - Good: `int_payments_pivoted_to_orders`, `int_orders_deduplicated`, `int_users_aggregated`
  - Bad: `int_payments`, `int_order_data`, `int_user_info`
- Drop the double underscore at this layer (moving toward business-conformed concepts)
- Exception: Keep double underscore when operating at source system level (e.g., `int_shopify__orders_summed`)

### Materialization
- Primary recommendation: ephemeral (keeps unnecessary models out of warehouse)
- Alternative: views in custom schemas (provides development insight, easier troubleshooting)
- Should generally NOT be exposed in the main production schema

### CTE Naming
- Use descriptive CTE names explaining what the transformation does
- Good: `pivot_and_aggregate_payments_to_order_grain`
- Bad: `tmp`, `cte1`, `final`

### DAG Architecture
- Design for multiple inputs (several arrows coming in)
- AVOID multiple outputs from a single model (several arrows going out is a red flag)
- Goal: move from numerous narrow concepts to fewer wider joined concepts

### What is FORBIDDEN
- Intermediate models should NOT be queried directly by end users
- Do NOT over-optimize too early
- Do NOT reference `source()` macro — intermediate models should only use `ref()` to staging or other intermediate models

### Testing
- Add primary key tests on re-grained models (critical)
- Include primary key tests on enriched models even when grain is unchanged (future-proofing)
- `accepted_values` tests on newly calculated categorical columns
- `not_constant` tests on continuously changing columns
- Consider unit testing for complex SQL logic
- Skip retesting passthrough columns already tested upstream

## Instructions
1. Read all models in `models/intermediate/`
2. Check each model against every rule above
3. Verify naming follows `int_[entity]s_[verb]ed` pattern with action verbs
4. Ensure no `source()` references exist — only `ref()`
5. Flag any models that look like they belong in staging (too simple) or marts (too final)
6. Check that complex logic from staging hasn't been left there instead of moved here
7. Report findings with specific file paths and line numbers
