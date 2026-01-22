with inventory as (
    select * from {{ ref('stg_inventory') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

-- Calculate recent sales velocity
recent_sales as (
    select
        oi.product_id,
        count(oi.order_item_id) as units_sold_30d,
        round(count(oi.order_item_id) / 30.0, 2) as avg_daily_sales
    from order_items oi
    where date(oi.created_at) >= date_sub(current_date(), interval 30 day)
    group by 1
),

-- Current inventory levels
current_inventory as (
    select
        product_id,
        count(*) as total_inventory,
        countif(sold_at is null) as available_inventory,
        countif(sold_at is not null) as sold_inventory
    from inventory
    group by 1
),

-- Combine with product details
inventory_forecast as (
    select
        p.product_id,
        p.product_name,
        p.brand,
        p.category,
        p.department,
        p.cost,
        p.retail_price,
        
        ci.total_inventory,
        ci.available_inventory,
        ci.sold_inventory,
        
        coalesce(rs.avg_daily_sales, 0) as avg_daily_sales_30d,
        coalesce(rs.units_sold_30d, 0) as units_sold_30d
        
    from products p
    left join current_inventory ci on p.product_id = ci.product_id
    left join recent_sales rs on p.product_id = rs.product_id
)

select
    *,
    -- Forecasting metrics
    case 
        when avg_daily_sales_30d > 0 
        then round(available_inventory / avg_daily_sales_30d, 1)
        else null
    end as days_of_stock_remaining,
    
    case
        when avg_daily_sales_30d > 0 and available_inventory / avg_daily_sales_30d < 7 
        then 'Critical - Reorder Now'
        when avg_daily_sales_30d > 0 and available_inventory / avg_daily_sales_30d < 14 
        then 'Low - Reorder Soon'
        when avg_daily_sales_30d > 0 and available_inventory / avg_daily_sales_30d < 30 
        then 'Normal'
        when avg_daily_sales_30d > 0 
        then 'Overstocked'
        else 'No Recent Sales'
    end as inventory_status,
    
    -- Recommended reorder quantity (30 days of stock)
    case
        when avg_daily_sales_30d > 0
        then greatest(0, round(avg_daily_sales_30d * 30 - available_inventory, 0))
        else 0
    end as recommended_reorder_qty,
    
    round((retail_price - cost) / nullif(retail_price, 0) * 100, 2) as margin_pct
    
from inventory_forecast
order by days_of_stock_remaining asc nulls last