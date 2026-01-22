with marketing as (
    select * from {{ ref('stg_marketing') }}
),

sessions as (
    select * from {{ ref('int_web_sessions') }}
),

orders as (
    select * from {{ ref('stg_order_items') }}
),

-- Aggregate session-level data first
session_summary as (
    select
        user_id,
        concat(cast(user_id as string), '-', cast(user_session_number as string)) as session_id,
        min(event_time) as session_start,
        traffic_source,
        countif(event_type = 'purchase') as purchases_in_session
    from sessions
    group by user_id, session_id, traffic_source
),

-- Join sessions with orders to get revenue
session_with_orders as (
    select
        ss.session_id,
        ss.user_id,
        ss.session_start,
        ss.traffic_source,
        count(distinct o.order_id) as orders_placed,
        coalesce(sum(o.sale_price), 0) as session_revenue
    from session_summary ss
    left join orders o 
        on ss.user_id = o.user_id
        and date(o.created_at) = date(ss.session_start)
        and ss.purchases_in_session > 0
    group by ss.session_id, ss.user_id, ss.session_start, ss.traffic_source
),

-- Match sessions to campaigns by date and platform
session_attribution as (
    select
        s.session_id,
        s.user_id,
        s.session_start,
        s.traffic_source,
        s.session_revenue,
        s.orders_placed,
        
        -- Try to match to campaign
        m.campaign_id,
        m.platform,
        m.spend as campaign_daily_spend,
        m.clicks as campaign_daily_clicks,
        
        -- Attribution logic
        case
            when s.traffic_source = 'Facebook' then 'Facebook'
            when s.traffic_source = 'Adwords' then 'Google'
            when s.traffic_source = 'YouTube' then 'YouTube'
            when s.traffic_source = 'Email' then 'Email'
            when s.traffic_source = 'Organic' then 'Organic Search'
            else 'Direct/Other'
        end as attributed_channel
        
    from session_with_orders s
    left join marketing m 
        on date(s.session_start) = m.date
        and (
            (s.traffic_source = 'Facebook' and m.platform = 'Facebook')
            or (s.traffic_source = 'Adwords' and m.platform = 'Google')
            or (s.traffic_source = 'YouTube' and m.platform = 'YouTube')
            or (s.traffic_source = 'Email' and m.platform = 'Email')
        )
)

select * from session_attribution