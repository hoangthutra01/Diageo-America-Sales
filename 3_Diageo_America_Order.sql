with cleansed_data as
(select *
from `tra-lam-data.public_data_cleanse.Update_Iowa_sales_cleansed`
)

,store_data as
(select distinct (store_number)
from cleansed_data
)

,month_data as
(select distinct (order_month)
from cleansed_data
)

select 
  invoice_and_item_number
  ,month_data.order_month
  ,store.store_number
  ,store.store_name
  ,city
  ,county
  ,category_number
  ,category_name
  ,vendor_number
  ,vendor_name
  ,item_number
  ,item_description
  ,wholesale_price_per_bottle
  ,sold_bottle
  ,order_amount
  ,volume_sold_liters
from store_data
cross join month_data
left join cleansed_data using (store_number,order_month)
left join `tra-lam-data.public_data_cleanse.diageo_america_range_date`
  as store
  using(store_number)
where month_data.order_month between '2021-01-01' and '2022-12-31'
  and vendor_number=260