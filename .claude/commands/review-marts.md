Review mart models in this dbt project against official dbt best practices.

## Rules to Check

### Purpose & Scope
- Marts are business-defined, consumable entities — the "entity layer" or "concept layer"
- Each mart embodies a specific entity at its unique grain (orders, customers, territories)
- One row per discrete instance of the entity
- Build wide and denormalized — storage is cheap, compute is expensive
- Pack every relevant concept into a single denormalized entity

### Structure & Organization
- Group by department or business area (finance, marketing, etc.)
- Use subfolders only when you have approximately 10+ marts
- File naming: plain English entity names, pluralized — `customers.sql`, `orders.sql`
- NO source system prefixes — these are business-conformed concepts

### Materialization
- Default: tables
- Use incremental only for large, compute-intensive datasets
- Progression: start as views → tables when slow to query → incremental when slow to build
- Do NOT make all marts incremental by default

### Joins & Complexity
- 8 staging models with simple joins = acceptable in a single mart
- 4 concepts with complex window functions = too much, add intermediate models
- If joining 4-5+ concepts with complex logic, break into intermediate models
- Two intermediate models (3 concepts each) + mart joining those = better than a single 6-join mart

### Cross-Mart Dependencies
- Building one mart upon another IS acceptable but requires careful consideration
- Example: `orders` mart used in `customers` mart to aggregate order data
- Must not create circular dependencies

### What is FORBIDDEN
- NO time-based rollups in mart names — `orders_per_day` belongs in metrics, not marts
- Do NOT build the same concept differently per department (`finance_orders` + `marketing_orders` is an anti-pattern)
  - Exception: Clearly separate concepts like `tax_revenue` vs `revenue` are acceptable
- Once you group entities along time dimensions, you have moved into metrics territory
- NO `source()` macro usage — marts should only use `ref()` to intermediate or staging models
- NO circular dependencies between marts

### Denormalization Rules
- Include all data from other concepts relevant to answering questions about the core entity
- Acceptable to duplicate data across marts (more efficient than repeated rejoins)
- Without Semantic Layer: denormalize heavily
- With Semantic Layer: stay as normalized as possible for MetricFlow flexibility

### Testing
- Unit tests on complex transformation logic (date calculations, customer segmentation)
- Primary key tests for grain changes
- Business anomaly tests on new calculated fields
- Singular tests on high-priority tables (e.g., fuzzy matching for duplicate emails)
- Percentage variance limits on calculated numerical fields
- Business rule validation (e.g., ledger daily totals must increase)
- Skip redundant tests on inherited columns already tested upstream

## Instructions
1. Read all models in `models/marts/`
2. Check each model against every rule above
3. Verify naming uses plain English pluralized entities (no source prefixes)
4. Flag any models with too many joins that should use intermediate models
5. Ensure no `source()` references — only `ref()`
6. Check materialization is table or incremental (not view or ephemeral)
7. Flag time-based rollups or per-department duplicates
8. Report findings with specific file paths and line numbers
