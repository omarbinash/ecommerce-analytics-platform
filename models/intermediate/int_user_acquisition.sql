with users as (
    select * from {{ ref('stg_users') }}
),

orders as (
    select * from {{ ref('stg_order_items') }}
),

user_value as (
    select
        user_id,
        count(distinct order_id) as total_orders,
        sum(sale_price) as lifetime_value,
        min(created_at) as first_order_date
    from orders
    group by 1
)

select
    u.traffic_source,
    count(distinct u.user_id) as total_users,
    count(distinct uv.user_id) as converted_users,
    round(count(distinct uv.user_id) / count(distinct u.user_id) * 100, 2) as conversion_rate,
    sum(uv.lifetime_value) as total_ltv,
    round(avg(uv.lifetime_value), 2) as avg_ltv,
    round(sum(uv.lifetime_value) / count(distinct u.user_id), 2) as ltv_per_user
from users u
left join user_value uv on u.user_id = uv.user_id
group by 1
order by total_ltv desc