Review SQL style in this dbt project against the official dbt SQL style guide.

## Rules to Check

### Formatting
- Indentation: 4 spaces (no tabs)
- Maximum line length: 80 characters
- All lowercase: field names, keywords, function names, types, literals
- Trailing commas on all field lists
- No trailing comma after the last field in a select

### Aliasing
- `as` keyword MUST be used explicitly for all column and table aliases
- Table aliases should be descriptive — avoid initialisms (no `o` for orders, use `orders`)

### Field Selection
- Fields MUST appear before aggregates and window functions in SELECT
- No `select *` anywhere in the project
- Columns should be organized: IDs, strings, numerics, booleans, dates, timestamps

### Naming
- snake_case for all schema, table, and column names
- Do NOT abbreviate — readability over brevity
- Primary keys: `<object>_id` (always string type)
- Booleans: prefix with `is_` or `has_`
- Timestamps: `<event>_at` (UTC), `<event>_at_<timezone>` for non-UTC
- Dates: `<event>_date`
- Date/time events: past tense verbs (created, updated, deleted)
- Price/revenue: decimal currency (not cents)
- Do NOT use reserved words as column names

### Joins
- Always write `inner join` not just `join`
- Prefer `union all` over `union` unless explicitly removing duplicates
- Always prefix column names with table/CTE name when joining multiple tables
- Avoid `right join` — swap table order and use `left join` instead
- Join logic should move left to right

### Grouping & Aggregation
- Use `group by 1, 2` (numeric) rather than repeating column names
- Perform aggregations early on the smallest possible datasets

### CTEs
- ALL `ref()` and `source()` calls go in import CTEs at the top of the file
- Name import CTEs after their referenced tables
- Functional CTEs should each perform one logical unit of work
- Use verbose descriptive CTE names (e.g., `events_joined_to_users`)
- Final line should be `select * from final_cte` for easy auditing — UNLESS project convention forbids `select *`
- Extract repeated CTE logic across models into separate intermediate models

### Comments
- Use Jinja comments `{# #}` for comments that should not appear in compiled SQL
- Use SQL comments `--` for comments that should appear in compiled SQL

### Jinja Style
- Spaces inside delimiters: `{{ this }}` not `{{this}}`
- Same for blocks: `{% block %}`, `{# comment #}`
- Use newlines to indicate logical blocks of Jinja
- Indent 4 spaces inside Jinja blocks

## Instructions
1. Read all `.sql` files in the `models/` directory recursively
2. Check each file against every rule above
3. Also run `uv run sqlfluff lint models/` and report any violations
4. Report findings with specific file paths and line numbers
5. Suggest fixes for any violations
