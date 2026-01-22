with attribution as (
    select * from {{ ref('int_campaign_attribution') }}
),

marketing as (
    select * from {{ ref('stg_marketing') }}
),

-- Only count sessions that actually matched to a campaign
campaign_sessions as (
    select
        date(session_start) as date,
        campaign_id,
        platform,
        COUNT(DISTINCT session_id) as attributed_sessions,
        COUNT(DISTINCT CASE WHEN orders_placed > 0 THEN session_id END) as converting_sessions,
        SUM(session_revenue) as attributed_revenue,
        SUM(orders_placed) as attributed_orders
    from attribution
    where campaign_id is not null  -- CRITICAL: Only include matched sessions
    group by 1, 2, 3
),

-- Join with marketing spend
campaign_metrics as (
    select
        m.date,
        m.campaign_id,
        m.platform,
        m.spend,
        m.clicks,
        m.impressions,
        
        -- Attribution metrics (use COALESCE in case no sessions matched)
        COALESCE(cs.attributed_sessions, 0) as attributed_sessions,
        COALESCE(cs.converting_sessions, 0) as converting_sessions,
        COALESCE(cs.attributed_revenue, 0) as attributed_revenue,
        COALESCE(cs.attributed_orders, 0) as attributed_orders
        
    from marketing m
    left join campaign_sessions cs
        on m.date = cs.date
        and m.campaign_id = cs.campaign_id
        and m.platform = cs.platform
)

select
    *,
    -- Performance metrics
    round(attributed_revenue / nullif(spend, 0), 2) as roas,
    round(spend / nullif(attributed_orders, 0), 2) as cpa,
    round(attributed_revenue / nullif(attributed_orders, 0), 2) as average_order_value,
    round(converting_sessions / nullif(clicks, 0) * 100, 2) as click_to_conversion_rate,
    round(clicks / nullif(impressions, 0) * 100, 2) as ctr,
    round(spend / nullif(clicks, 0), 2) as cpc
    
from campaign_metrics
order by date desc, attributed_revenue desc