with diageo_america_store as
(select
  store_number 
  , store_name 
  , date_trunc(min (order_date), month) as first_order_date
  , date_trunc(max (order_date), month) as last_order_date
from `tra-lam-data.public_data_cleanse.Iowa_liquor_sales_cleanced`
where 
  vendor_number =260
group by 1, 2
order by 1,2
)

select *
from diageo_america_store