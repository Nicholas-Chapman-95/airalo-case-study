{# Incremental strategy: when an order changes status, all orders   #}
{# for that user are re-processed to ensure customer sequencing     #}
{# columns (attempt_number, purchase_number, etc.) remain correct   #}
{# across the user's full order history.                            #}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["status", "destination_country"]
    )
}}

with {% if is_incremental() %}
    changed_users as (

        select distinct src.user_id
        from {{ ref('int_orders_customer_sequenced') }} as src
        where src.updated_at > (
            select max(wm.updated_at) as _wm
            from {{ this }} as wm
        )

    ),

{% endif %}orders as (

    select *
    from {{ ref('int_orders_customer_sequenced') }} as src
    {%- if is_incremental() %}
        where src.user_id in (
            select changed_users.user_id
            from changed_users
        )
    {%- endif %}

),

users as (

    select * from {{ ref('stg_raw__users') }}

),

countries as (

    select * from {{ ref('seed_countries') }}

)

select
    -- ids
    orders.order_id,
    orders.user_id,

    -- order attributes
    orders.currency,
    orders.card_country,
    countries_card.country_name as card_country_name,
    orders.destination_country,
    countries_dest.country_name as destination_country_name,
    orders.payment_method,
    orders.status,
    orders.esim_package,
    orders.package_data_gb,
    orders.package_validity_days,
    orders.is_unlimited_package,
    orders.is_completed,
    orders.is_refunded,
    orders.is_failed,

    -- revenue
    orders.amount,
    {{ convert_to_usd('orders.amount', 'orders.currency') }}
        as amount_usd,

    -- attempt-level metrics (populated for all orders)
    orders.customer_attempt_number,
    orders.is_first_attempt,

    -- purchase-level metrics (null for failed orders)
    orders.customer_purchase_number,
    orders.is_first_purchase,
    orders.previous_purchase_at,
    orders.days_since_previous_purchase,
    orders.customer_first_purchase_at,
    orders.days_since_first_purchase,

    -- user dimensions
    users.platform as user_platform,
    users.acquisition_channel as user_acquisition_channel,
    users.ip_country as user_ip_country,
    countries_user.country_name as user_ip_country_name,
    users.created_at as user_created_at,

    -- timestamps
    orders.created_at,
    orders.updated_at

from orders
left join users
    on orders.user_id = users.user_id
left join countries as countries_card
    on orders.card_country = countries_card.country_code
left join countries as countries_dest
    on orders.destination_country = countries_dest.country_code
left join countries as countries_user
    on users.ip_country = countries_user.country_code
