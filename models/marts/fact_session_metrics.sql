with sessions as (
    select * from {{ ref('int_web_sessions') }}
),

orders as (
    select * from {{ ref('stg_order_items') }}
),

session_aggregates as (
    select
        session_id,
        user_id,
        min(event_time) as session_start,
        max(event_time) as session_end,
        timestamp_diff(max(event_time), min(event_time), minute) as session_duration_minutes,
        count(event_id) as total_events,
        count(distinct event_type) as unique_event_types,
        
        -- Take first value for session-level attributes
        any_value(traffic_source) as traffic_source,
        any_value(browser) as browser,
        any_value(state) as state,
        any_value(city) as city,
        
        -- Conversion funnel metrics
        countif(event_type = 'home') as home_views,
        countif(event_type = 'product') as product_views,
        countif(event_type = 'cart') as cart_adds,
        countif(event_type = 'purchase') as purchases,
        
        -- Landing page
        array_agg(uri order by event_time limit 1)[offset(0)] as landing_page,
        array_agg(uri order by event_time desc limit 1)[offset(0)] as exit_page
        
    from sessions
    group by session_id, user_id
),

-- Join with actual orders to get revenue
session_with_revenue as (
    select
        sa.*,
        count(distinct o.order_id) as orders_placed,
        sum(o.sale_price) as session_revenue
    from session_aggregates sa
    left join orders o 
        on sa.user_id = o.user_id
        and timestamp_trunc(o.created_at, day) = timestamp_trunc(sa.session_start, day)
    group by 
        sa.session_id,
        sa.user_id,
        sa.session_start,
        sa.session_end,
        sa.session_duration_minutes,
        sa.total_events,
        sa.unique_event_types,
        sa.traffic_source,
        sa.browser,
        sa.state,
        sa.city,
        sa.home_views,
        sa.product_views,
        sa.cart_adds,
        sa.purchases,
        sa.landing_page,
        sa.exit_page
)

select
    *,
    -- Conversion rates
    case 
        when product_views > 0 
        then round(cart_adds / product_views * 100, 2) 
        else 0 
    end as add_to_cart_rate,
    
    case 
        when cart_adds > 0 
        then round(purchases / cart_adds * 100, 2) 
        else 0 
    end as cart_to_purchase_rate,
    
    case 
        when product_views > 0 
        then round(purchases / product_views * 100, 2) 
        else 0 
    end as overall_conversion_rate,
    
    -- Engagement
    case 
        when session_duration_minutes > 0 
        then round(total_events / session_duration_minutes, 2) 
        else total_events 
    end as events_per_minute,
    
    case 
        when session_revenue > 0 then true 
        else false 
    end as is_converting_session
    
from session_with_revenue
order by session_start desc