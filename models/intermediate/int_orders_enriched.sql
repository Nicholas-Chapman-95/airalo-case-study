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
    u.platform as user_platform,
    u.acquisition_channel as user_acquisition_channel,
    u.ip_country as user_ip_country,
    uc.country_name as user_ip_country_name,
    u.created_at as user_signup_at,

    -- financials
    er.usd_rate,
    o.amount as order_amount_local,
    o.amount / er.usd_rate as order_amount_usd

from {{ ref('int_orders_latest') }} as o
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
