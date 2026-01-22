with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        p.product_id,
        p.product_name,
        p.brand,
        p.category,
        p.department,
        p.cost,
        p.retail_price,
        count(distinct oi.order_id) as total_orders,
        sum(oi.sale_price) as total_revenue,
        count(oi.order_item_id) as units_sold,
        sum(oi.sale_price - p.cost) as gross_profit,
        round(avg(oi.sale_price), 2) as avg_sale_price
    from order_items oi
    inner join products p on oi.product_id = p.product_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    *,
    round(gross_profit / nullif(total_revenue, 0) * 100, 2) as gross_margin_pct,
    round(total_revenue / nullif(units_sold, 0), 2) as revenue_per_unit
from product_sales
order by total_revenue desc