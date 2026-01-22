with orders as (
    select * from {{ ref('stg_order_items') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

first_purchase as (
    select
        user_id,
        date_trunc(min(date(created_at)), month) as cohort_month
    from orders
    group by 1
),

cohort_orders as (
    select
        fp.user_id,
        fp.cohort_month,
        date_trunc(date(o.created_at), month) as order_month,
        date_diff(
            date_trunc(date(o.created_at), month),
            fp.cohort_month,
            month
        ) as months_since_first_purchase,
        o.sale_price
    from first_purchase fp
    inner join orders o on fp.user_id = o.user_id
),

cohort_summary as (
    select
        cohort_month,
        months_since_first_purchase,
        count(distinct user_id) as customers_active,  -- Changed name for clarity
        sum(sale_price) as revenue
    from cohort_orders
    group by 1, 2
),

cohort_size as (
    select
        cohort_month,
        count(distinct user_id) as cohort_size
    from first_purchase
    group by 1
)

select
    cs.cohort_month,
    cs.months_since_first_purchase,
    cs.customers_active,
    css.cohort_size,
    round(cs.customers_active * 100.0 / css.cohort_size, 2) as retention_rate,  -- Fixed calculation
    cs.revenue,
    round(cs.revenue / cs.customers_active, 2) as avg_order_value
from cohort_summary cs
left join cohort_size css on cs.cohort_month = css.cohort_month
order by cohort_month, months_since_first_purchase