
with rename_column as
(select
  invoice_and_item_number
  ,date as order_date
  ,store_number
  ,store_name
  ,city
  ,county
  ,category as category_number
  ,category_name
  ,vendor_number
  ,vendor_name
  ,item_number
  ,item_description
  ,state_bottle_retail as wholesale_price_per_bottle
  ,bottles_sold as sold_bottle
  ,sale_dollars as order_amount
  ,volume_sold_liters
from `vit-lam-data.public_data_sample.iowa_liquor_sales`
)

,cast_type as
(select 
  invoice_and_item_number
  ,order_date
  ,date_trunc(order_date, month) as order_month
  ,cast (store_number as integer) as store_number
  ,store_name
  ,city
  ,county
  ,cast (replace(category_number,'.0','') as integer) as category_number
  ,category_name
  ,cast (replace(vendor_number,'.0','') as integer) as vendor_number
  ,vendor_name
  ,cast (item_number as integer) as item_number
  ,item_description
  ,cast (wholesale_price_per_bottle as numeric) as wholesale_price_per_bottle
  ,sold_bottle
  ,cast (order_amount as numeric) as order_amount
  ,cast (volume_sold_liters as numeric) as volume_sold_liters
from rename_column
)

,rank_no as
(select  
  *
  ,row_number() over (partition by store_number order by order_date desc) as rank_store
  ,row_number() over (partition by vendor_number order by order_date desc) as rank_vendor
  ,row_number() over (partition by category_number order by order_date desc) as rank_category
  ,row_number() over (partition by item_number order by order_date desc) as rank_item
from cast_type
)

,store as
(select
  store_number 
  , store_name
from rank_no
where rank_store=1
)

,vendor as
(select 
  vendor_number
  ,vendor_name
from rank_no
where rank_vendor=1
)

,item as
(select 
  item_number
  ,item_description
from rank_no
where rank_item=1
)

,category as
(select 
  category_number
  ,category_name
from rank_no
where rank_category=1
)

select 
  invoice_and_item_number
  ,order_month
  ,store_number
  ,store.store_name
  ,city
  ,county
  ,category.category_number
  ,category.category_name
  ,vendor_number
  ,vendor.vendor_name
  ,item_number
  ,item.item_description
  ,wholesale_price_per_bottle
  ,sold_bottle
  ,order_amount
  ,volume_sold_liters
from cast_type
left join store using (store_number)
left join category using (category_number)
left join vendor using (vendor_number)
left join item using (item_number)