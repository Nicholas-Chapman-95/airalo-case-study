{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["order_id"],
        on_schema_change='append_new_columns'
    )
}}

with latest as (

    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as row_num
    from {{ ref('stg_raw__orders') }}

    {% if is_incremental() %}
        where updated_at >= timestamp('{{ var("data_interval_start") }}')
          and updated_at < timestamp('{{ var("data_interval_end") }}')
    {% endif %}

)

select
    order_id,
    user_id,
    currency,
    card_country,
    destination_country,
    payment_method,
    status,
    esim_package,
    package_data_gigabytes,
    package_validity_days,
    is_unlimited_package,
    is_completed,
    is_refunded,
    is_failed,
    amount,
    created_at,
    updated_at
from latest
where row_num = 1
