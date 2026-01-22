with source as (
    select * from {{ source('thelook', 'products') }}
),

renamed as (
    select
        id as product_id,
        cost,
        category,
        name as product_name,
        brand,
        retail_price,
        department,
        sku
    from source
)

select * from renamed