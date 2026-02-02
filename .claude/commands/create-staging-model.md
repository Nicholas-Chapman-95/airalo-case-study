Create a new staging model for the specified source table: $ARGUMENTS

## Requirements

Follow ALL official dbt best practices for staging models:

### Before Creating
1. Check if the source table is already defined in `_sources.yml` — if not, add it
2. Check if a staging model already exists for this table — do not create duplicates
3. Query the source table schema using `bq show --format=json` to get all columns and types

### Model Structure
- File name: `stg_[source]__[entity]s.sql` (double underscore, pluralized)
- Place in `models/staging/` under the appropriate source subdirectory

### Required Pattern
```sql
with source as (

    select
        column1,
        column2
    from {{ source('source_name', 'table_name') }}

)

select
    -- Explicitly list and transform every column
    -- Order: IDs, strings, numerics, booleans, dates, timestamps
from source
```

### Transformation Rules
- Explicitly select all columns — NO `select *`
- Rename columns for clarity using `as`
- Cast types appropriately:
  - Primary keys → string
  - Amounts/prices → numeric (convert cents to dollars if needed)
  - Timestamps → timestamp
- Apply naming conventions:
  - Primary keys: `<object>_id`
  - Booleans: `is_` or `has_` prefix
  - Timestamps: `<event>_at`
  - Dates: `<event>_date`
  - Price/revenue: decimal currency

### Forbidden in Staging
- NO joins
- NO aggregations
- NO deduplication logic
- NO complex business logic
- NO window functions for business logic

### After Creating
1. Run `uv run sqlfluff lint` on the new file and fix any violations
2. Add appropriate tests to the schema YAML:
   - Primary key: unique + not_null
   - Business anomaly tests where appropriate
3. Run `uv run dbt compile --select` on the model to verify it compiles
