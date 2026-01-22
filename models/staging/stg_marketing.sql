with source as (
    select * from {{ source('raw', 'marketing_spend') }}
),

renamed as (
    select
        date,
        campaign_id,
        platform,
        spend,
        clicks,
        impressions
    from source
)

select * from renamed