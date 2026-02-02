with source as (

    select
        currency,
        usd_rate
    from {{ source('raw', 'exchange_rates') }}

)

select
    cast(currency as string) as currency,
    cast(usd_rate as numeric) as usd_rate

from source
