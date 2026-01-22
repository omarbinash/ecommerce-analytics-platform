with daily_pnl as (
    select * from {{ ref('fact_daily_pnl') }}
),

with_moving_avg as (
    select
        *,
        avg(total_revenue) over (
            order by date 
            rows between 6 preceding and current row
        ) as revenue_7day_ma,
        avg(total_ad_spend) over (
            order by date 
            rows between 6 preceding and current row
        ) as spend_7day_ma,
        avg(roas) over (
            order by date 
            rows between 6 preceding and current row
        ) as roas_7day_ma,
        sum(total_revenue) over (
            order by date 
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        sum(total_ad_spend) over (
            order by date 
            rows between unbounded preceding and current row
        ) as cumulative_spend
    from daily_pnl
)

select
    *,
    round((total_revenue - revenue_7day_ma) / nullif(revenue_7day_ma, 0) * 100, 2) as revenue_vs_7day_avg_pct,
    round(cumulative_revenue - cumulative_spend, 2) as cumulative_profit
from with_moving_avg
order by date