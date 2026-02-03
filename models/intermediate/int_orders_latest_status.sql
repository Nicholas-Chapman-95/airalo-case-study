-- merge strategy: the incremental filter is on updated_at
-- (when the status changed) but the partition key is created_at
-- (when the order was placed). a status change today can affect
-- an order in any historical partition, so we need row-level
-- upserts. insert_overwrite / microbatch would drop and replace
-- entire partitions, destroying sibling rows.
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
        cluster_by=["order_id", "user_id"],
        on_schema_change='append_new_columns'
    )
}}

with source_orders as (

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
        updated_at,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as row_num
    from {{ ref('stg_raw__orders') }}

    {% if is_incremental() %}
        -- expects Airflow to pass data_interval_start and
        -- data_interval_end as dbt vars (half-open interval
        -- [start, end) so each run processes exactly one
        -- non-overlapping batch of status changes)
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
from source_orders
where row_num = 1
