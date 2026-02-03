with source as (

    select
        user_id,
        created_at,
        platform,
        acquisition_channel,
        ip_country
    from {{ source('raw', 'users') }}

)

select
    -- ids: cast to string for join safety and
    -- surrogate key compatibility with orders
    cast(user_id as string) as user_id,

    -- platform: source has mixed case ('iOS', 'android')
    -- — normalize to lowercase for consistent downstream filters
    lower(trim(platform)) as platform,

    -- acquisition_channel: source has mixed case with spaces
    -- (e.g. 'Referral Link') — normalize to snake_case to
    -- match payment_method convention in stg_raw__orders
    lower(trim(replace(acquisition_channel, ' ', '_')))
        as acquisition_channel,

    -- ip_country: ISO 3166-1 alpha-2 codes are uppercase
    -- by convention; upper + trim keeps consistency with
    -- card_country / destination_country in stg_raw__orders
    upper(trim(ip_country)) as ip_country,

    -- timestamps: explicit cast from source string to
    -- timestamp for type safety in downstream date logic
    cast(created_at as timestamp) as created_at

from source
