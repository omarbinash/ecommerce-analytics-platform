with marketing as (
    select * from {{ ref('stg_marketing') }}
),

daily_marketing as (
    select
        date,
        sum(spend) as total_spend,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions
    from marketing
    group by 1
)

select * from daily_marketing