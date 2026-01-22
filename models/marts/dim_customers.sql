with users as (
    select * from {{ ref('stg_users') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customer_stats as (
    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.country,
        u.traffic_source,
        u.created_at as customer_since,
        count(distinct oi.order_id) as total_orders,
        sum(oi.sale_price) as lifetime_value,
        min(oi.created_at) as first_order_date,
        max(oi.created_at) as last_order_date,
        date_diff(current_date(), date(max(oi.created_at)), day) as days_since_last_order
    from users u
    left join order_items oi on u.user_id = oi.user_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select 
    *,
    case
        when days_since_last_order <= 30 then 'Active'
        when days_since_last_order <= 90 then 'At Risk'
        when days_since_last_order > 90 then 'Churned'
        else 'Never Purchased'
    end as customer_status
from customer_stats