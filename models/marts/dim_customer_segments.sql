with customer_stats as (
    select * from {{ ref('dim_customers') }}
),

rfm_calc as (
    select
        user_id,
        first_name,
        last_name,
        email,
        total_orders as frequency,
        lifetime_value as monetary,
        days_since_last_order as recency,
        ntile(5) over (order by days_since_last_order asc) as r_score,
        ntile(5) over (order by total_orders desc) as f_score,
        ntile(5) over (order by lifetime_value desc) as m_score
    from customer_stats
    where total_orders > 0
)

select
    *,
    (r_score + f_score + m_score) as rfm_score,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 3 and f_score >= 3 and m_score >= 3 then 'Loyal Customers'
        when r_score >= 4 and f_score <= 2 and m_score <= 2 then 'New Customers'
        when r_score >= 3 and f_score <= 3 and m_score <= 3 then 'Potential Loyalists'
        when r_score <= 2 and f_score >= 3 and m_score >= 3 then 'At Risk'
        when r_score <= 2 and f_score >= 4 and m_score >= 4 then 'Cant Lose Them'
        when r_score <= 2 and f_score <= 2 and m_score >= 3 then 'Hibernating High Value'
        else 'Lost'
    end as customer_segment
from rfm_calc
order by rfm_score desc