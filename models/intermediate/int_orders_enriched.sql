-- merge strategy: the incremental filter is on updated_at
-- (when the status changed) but the partition key is order_created_at
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
            "field": "order_created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["order_id", "user_id"],
        on_schema_change='append_new_columns'
    )
}}

-- pull the latest-status version of each order from the
-- deduplication layer. on incremental runs, only pick up
-- orders whose status changed within the current batch
-- window (controlled by Airflow vars). this keeps the
-- merge target small — only changed rows get re-joined
-- and upserted rather than reprocessing the full table.
with latest_orders as (

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
    from {{ ref('int_orders_latest_status') }}

    {% if is_incremental() %}
        -- incremental filter: half-open interval [start, end)
        -- matches the Airflow batch window so each run processes
        -- exactly one non-overlapping slice of status changes
        where updated_at >= timestamp('{{ var("data_interval_start") }}')
          and updated_at < timestamp('{{ var("data_interval_end") }}')
    {% endif %}

)

select
    -- order dimensions
    o.order_id,
    o.currency as order_currency,
    o.card_country as order_card_country,
    cc.country_name as order_card_country_name,
    o.destination_country as order_destination_country,
    dc.country_name as order_destination_country_name,
    o.payment_method as order_payment_method,
    o.status as order_status,
    o.esim_package as order_esim_package,
    o.package_data_gigabytes as order_package_data_gigabytes,
    o.package_validity_days as order_package_validity_days,
    o.is_unlimited_package as order_is_unlimited_package,
    o.is_completed as order_is_completed,
    o.is_refunded as order_is_refunded,
    o.is_failed as order_is_failed,
    o.created_at as order_created_at,
    o.updated_at as order_updated_at,

    -- user dimensions (user_id from orders, not users,
    -- so it's never null even if the user is missing)
    o.user_id,
    -- user attributes are signup-time metadata (one row per
    -- user, no updated_at in source). if the source system
    -- silently overwrites these fields, tracking changes
    -- would require a dbt snapshot on raw.users
    u.platform as user_signup_platform,
    u.acquisition_channel as user_signup_acquisition_channel,
    u.ip_country as user_signup_ip_country,
    uc.country_name as user_signup_ip_country_name,
    u.created_at as user_signup_at,

    -- financials
    er.usd_rate,
    o.amount as order_amount_local,
    o.amount / nullif(er.usd_rate, 0) as order_amount_usd

from latest_orders as o
left join {{ ref('stg_raw__users') }} as u
    on o.user_id = u.user_id
left join {{ ref('stg_raw__exchange_rates') }} as er
    on o.currency = er.currency
left join {{ ref('seed_countries') }} as cc
    on o.card_country = cc.country_code
left join {{ ref('seed_countries') }} as dc
    on o.destination_country = dc.country_code
left join {{ ref('seed_countries') }} as uc
    on u.ip_country = uc.country_code
