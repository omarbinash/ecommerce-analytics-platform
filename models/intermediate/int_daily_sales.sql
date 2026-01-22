with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

daily_sales as (
    select
        date(oi.created_at) as date,
        count(distinct oi.order_id) as total_orders,
        sum(oi.sale_price) as total_revenue,
        count(oi.order_item_id) as items_sold
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    where o.status not in ('Cancelled', 'Returned')
    group by 1
)

select * from daily_sales