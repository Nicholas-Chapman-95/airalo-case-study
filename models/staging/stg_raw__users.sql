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
    cast(user_id as string) as user_id,

    -- normalize casing for consistent downstream filters
    lower(trim(platform)) as platform,
    lower(trim(replace(acquisition_channel, ' ', '_')))
        as acquisition_channel,

    -- ISO 3166-1 alpha-2 codes are uppercase by convention;
    -- matches seed_countries.csv for downstream joins
    upper(trim(ip_country)) as ip_country,

    -- timestamps
    cast(created_at as timestamp) as created_at

from source
