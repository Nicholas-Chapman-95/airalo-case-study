{% macro convert_to_usd(amount_column, currency_column, timestamp_column=none) %}
    {{ amount_column }} / (
        select usd_rate
        from {{ ref('snp_raw__exchange_rates') }}
        where currency = {{ currency_column }}
        {% if timestamp_column is not none %}
            and {{ timestamp_column }} >= dbt_valid_from
            and ({{ timestamp_column }} < dbt_valid_to or dbt_valid_to is null)
        {% else %}
            and dbt_valid_to is null
        {% endif %}
    )
{% endmacro %}
