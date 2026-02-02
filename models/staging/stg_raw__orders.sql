with source as (

    select
        order_id,
        user_id,
        created_at,
        updated_at,
        amount,
        currency,
        esim_package,
        payment_method,
        card_country,
        destination_country,
        status
    from {{ source('raw', 'orders') }}

)

select
    -- surrogate key: composite of order_id + updated_at
    -- since orders have multiple rows per status change
    {{ dbt_utils.generate_surrogate_key(
        ['order_id', 'updated_at']
    ) }} as order_status_key,

    -- ids: cast to string for join safety and
    -- surrogate key compatibility
    cast(order_id as string) as order_id,
    cast(user_id as string) as user_id,

    -- strings: iso codes (4217 / 3166-1 alpha-2)
    -- upper + trim for consistency with ip_country
    upper(trim(currency)) as currency,
    upper(trim(card_country)) as card_country,
    upper(trim(destination_country)) as destination_country,

    -- payment_method: source has mixed case with
    -- spaces (e.g. 'Apple Pay') — normalize to
    -- snake_case for consistent downstream filters
    lower(trim(replace(payment_method, ' ', '_')))
        as payment_method,

    -- status: lowercase for consistency
    lower(trim(status)) as status,

    -- esim_package: keep original label, then parse
    -- into components so downstream models don't
    -- repeat the regex. format is 'NGB - N Days'
    -- or 'Unlimited'
    trim(esim_package) as esim_package,
    cast(
        regexp_extract(esim_package, r'(\d+)GB')
        as int64
    ) as package_data_gigabytes,
    -- validity_days: null for unlimited plans
    cast(
        regexp_extract(
            esim_package, r'(\d+) Days'
        ) as int64
    ) as package_validity_days,
    trim(esim_package) = 'Unlimited'
        as is_unlimited_package,

    -- status booleans: common downstream filters,
    -- also convenient for sum() aggregations
    lower(trim(status)) = 'completed' as is_completed,
    lower(trim(status)) = 'refunded' as is_refunded,
    lower(trim(status)) = 'failed' as is_failed,

    -- numerics: cast to numeric (not float64) to
    -- avoid floating-point precision errors on
    -- currency calculations
    cast(amount as numeric) as amount,

    -- timestamps
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from source
