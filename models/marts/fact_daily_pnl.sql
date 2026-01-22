with sales as (
    select * from {{ ref('int_daily_sales') }}
),

marketing as (
    select * from {{ ref('int_daily_marketing') }}
),

daily_pnl as (
    select
        coalesce(s.date, m.date) as date,
        coalesce(s.total_revenue, 0) as total_revenue,
        coalesce(s.total_orders, 0) as total_orders,
        coalesce(s.items_sold, 0) as items_sold,
        coalesce(m.total_spend, 0) as total_ad_spend,
        coalesce(m.total_clicks, 0) as total_clicks,
        coalesce(m.total_impressions, 0) as total_impressions,
        -- Calculated metrics
        coalesce(s.total_revenue, 0) - coalesce(m.total_spend, 0) as gross_profit,
        case 
            when coalesce(m.total_spend, 0) > 0 
            then round(coalesce(s.total_revenue, 0) / m.total_spend, 2)
            else null 
        end as roas,
        case 
            when coalesce(s.total_orders, 0) > 0 
            then round(coalesce(m.total_spend, 0) / s.total_orders, 2)
            else null 
        end as cpa
    from sales s
    full outer join marketing m on s.date = m.date
)

select * from daily_pnl
order by date