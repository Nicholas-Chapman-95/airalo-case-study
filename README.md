# Airalo dbt

dbt project for Airalo, targeting BigQuery.

## Prerequisites

- [UV](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- Google Cloud credentials configured for BigQuery access

## Setup

Install dependencies (creates a virtual environment automatically):

```bash
uv sync
```

Initialize the dbt project (first time only):

```bash
uv run dbt init
```

Verify your connection:

```bash
uv run dbt debug
```

## Common Commands

```bash
# Install dbt packages from packages.yml
uv run dbt deps

# Run all models
uv run dbt run

# Run a specific model
uv run dbt run --select model_name

# Run tests
uv run dbt test

# Generate and serve docs
uv run dbt docs generate
uv run dbt docs serve

# Compile SQL without running
uv run dbt compile

# Fresh run (full refresh of incremental models)
uv run dbt run --full-refresh

# List resources
uv run dbt ls
```

## Adding Dependencies

```bash
uv add <package-name>
```
