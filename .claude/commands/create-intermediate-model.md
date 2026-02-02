Create a new intermediate model for the specified purpose: $ARGUMENTS

## Requirements

Follow ALL official dbt best practices for intermediate models:

### Before Creating
1. Identify which staging models will be referenced
2. Determine the transformation type: structural simplification, re-graining, or isolating complexity
3. Verify the logic does not belong in staging (too simple) or marts (too final)

### Model Structure
- File name: `int_[entity]s_[verb]ed.sql` — MUST include action verb describing transformation
  - Good examples: `int_orders_deduplicated`, `int_payments_pivoted_to_orders`, `int_users_aggregated`
  - Bad examples: `int_orders`, `int_user_data`
- Place in `models/intermediate/` under appropriate business function subdirectory
- Drop the double underscore (moving toward business-conformed concepts)

### Required Pattern
```sql
with model_name as (

    select
        column1,
        column2
    from {{ ref('stg_source__entity') }}

),

transformation_description as (

    select
        -- transformed columns
    from model_name

)

select
    -- final column list
from transformation_description
```

### Rules
- ONLY use `ref()` — never `source()`
- Use descriptive CTE names explaining the transformation
- Each CTE should perform one logical unit of work
- Explicitly list columns — no `select *` (unless final CTE for auditability)
- Design for multiple inputs (several refs coming in)
- Avoid multiple downstream dependents where possible

### Appropriate Use Cases
1. Structural simplification — joining 4-6 staging concepts
2. Re-graining — fan out or collapse to correct grain
3. Isolating complexity — deduplication, pivots, complex window functions

### Materialization
- Default to ephemeral (set in dbt_project.yml or model config)
- Use views if troubleshooting visibility is needed

### After Creating
1. Run `uv run sqlfluff lint` on the new file and fix any violations
2. Add to `_int_[business_function]__models.yml`:
   - Primary key test (unique + not_null), especially for re-grained models
   - `accepted_values` on new categorical columns
3. Run `uv run dbt compile --select` on the model to verify it compiles
