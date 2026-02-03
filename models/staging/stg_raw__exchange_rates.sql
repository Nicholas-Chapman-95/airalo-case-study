{{ config(tags=['daily_6am']) }}

with source as (

    select
        currency,
        usd_rate
    from {{ source('raw', 'exchange_rates') }}

)

select
    -- currency: cast to string so it can join directly
    -- against stg_raw__orders.currency without implicit
    -- type coercion
    cast(currency as string) as currency,

    -- usd_rate: cast to numeric (not float64) to avoid
    -- floating-point precision errors when multiplying
    -- against order amounts downstream
    cast(usd_rate as numeric) as usd_rate

from source
