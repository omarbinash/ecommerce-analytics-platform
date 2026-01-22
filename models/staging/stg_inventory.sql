with source as (
    select * from {{ source('thelook', 'inventory_items') }}
),

renamed as (
    select
        id as inventory_item_id,
        product_id,
        created_at,
        sold_at,
        cost,
        product_category,
        product_name,
        product_brand,
        product_department
    from source
)

select * from renamed