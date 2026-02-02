{# raw.orders contains multiple rows per order_id due to status changes. #}
{# We take the most recent row per order_id using updated_at. #}

with orders_ranked_by_recency as (

    select
        order_id,
        user_id,
        currency,
        esim_package,
        payment_method,
        card_country,
        destination_country,
        status,
        package_data_gb,
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

)

select
    order_id,
    user_id,
    currency,
    esim_package,
    payment_method,
    card_country,
    destination_country,
    status,
    package_data_gb,
    package_validity_days,
    is_unlimited_package,
    is_completed,
    is_refunded,
    is_failed,
    amount,
    created_at,
    updated_at
from orders_ranked_by_recency
where row_num = 1
