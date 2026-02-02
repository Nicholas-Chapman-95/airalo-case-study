Create a new mart model for the specified business entity: $ARGUMENTS

## Requirements

Follow ALL official dbt best practices for mart models:

### Before Creating
1. Identify which intermediate and/or staging models will be referenced
2. Define the grain — what does one row represent?
3. If joining 4-5+ concepts with complex logic, create intermediate models first

### Model Structure
- File name: plain English, pluralized — `customers.sql`, `orders.sql`
- NO source system prefixes
- Place in `models/marts/` under appropriate business function subdirectory

### Required Pattern
```sql
with entity_name as (

    select
        column1,
        column2
    from {{ ref('int_entity_transformed') }}

),

another_entity as (

    select
        column1,
        column2
    from {{ ref('int_another_entity') }}

),

final as (

    select
        -- Build wide and denormalized
        -- Include all relevant data from joined concepts
        -- One row per discrete instance of the entity
    from entity_name
    inner join another_entity
        on entity_name.id = another_entity.id

)

select
    -- final column list
from final
```

### Rules
- ONLY use `ref()` — never `source()`
- Build wide and denormalized — storage is cheap, compute is expensive
- One row per discrete instance of the entity (enforce grain)
- Include all data relevant to answering questions about the core entity
- Duplicating data across marts is acceptable and preferred over repeated rejoins
- Use explicit `inner join` / `left join` — never bare `join` or `right join`
- Always prefix columns with table/CTE name when joining

### Complexity Guidelines
- 8 staging models with simple joins = acceptable
- 4 concepts with complex window functions = too much, use intermediate models
- If too complex, break into intermediate models first

### What is FORBIDDEN
- NO time-based rollups in model names (`orders_per_day` = metrics, not marts)
- NO per-department concept duplication (`finance_orders` + `marketing_orders`)
- NO `source()` references
- NO circular dependencies with other marts

### Materialization
- Default to table
- Use incremental only for large, compute-intensive datasets
- Never use view or ephemeral for marts

### After Creating
1. Run `uv run sqlfluff lint` on the new file and fix any violations
2. Add to `_[business_function]__models.yml`:
   - Primary key: unique + not_null
   - Unit tests on complex transformation logic
   - Business anomaly tests on calculated fields
   - Percentage variance limits on numerical fields
3. Run `uv run dbt compile --select` on the model to verify it compiles
4. Run `uv run dbt run --select` on the model to build it
