--Need to count the number of new and lost customers each month.
--Define new customers and lost customers:
---New customers are the ones who have the first order. They are considered as a new customer in that month.
---Lost customers are the ones who have no orders in EXACTLY 3 consecutive months.

with order_quantity_month as
(select 
  store_number 
  ,order_month
  ,count (invoice_and_item_number) as count_lines
from `tra-lam-data.public_data_cleanse.Update_Iowa_sales_cleansed`
where vendor_number=260
group by 1,2
order by 1,2
)

,store_data as
(select distinct (store_number)
from order_quantity_month
)

,month_data as
(select distinct (order_month)
from order_quantity_month
)

,store_by_month_densed as
(select 
  store_data.store_number
  ,month_data.order_month
  ,coalesce(order_quantity_month.count_lines,0) as count_lines
  ,store.first_order_date
  ,store.last_order_date
from store_data
cross join month_data
left join order_quantity_month using (store_number,order_month)
left join `tra-lam-data.public_data_cleanse.diageo_america_range_date`
  as store
  using(store_number)
where month_data.order_month between store.first_order_date and store.last_order_date + interval 3 month
 --month_data.order_month được nối rộng thêm 3 tháng so với ngày order cuối cùng của một store vì ta cần ghi nhận store này đã lost ở tháng nào. Nếu đừng lại ở tháng order cuối cùng thì store sẽ mãi ở trạng thái active dù đến hay store đó không còn đặt hàng nữa. Với khoảng thời gian nới thêm, ta có thể ghi nhận store ở trạng thái lost ở tháng thứ 3 kể từ tháng order cuối cùng.
order by 1,2
)

,calculate_L3M as
(select 
  *
  ,sum (count_lines) over (partition by store_number order by order_month rows between 2 preceding and current row) as count_lines_L3M
  ,sum (count_lines) over (partition by store_number order by order_month rows between 3 preceding and 1 preceding) as count_lines_L4M
from store_by_month_densed
)

,store_by_month_final as
(select
  * except (first_order_date,last_order_date)
  ,case
    when first_order_date=order_month then 'New'
    when count_lines_L4M=0 and count_lines_L3M<>0 then 'Returning'
    --Store ở trạng thái Returning khi nó vừa vào trạng thái Lost ở tháng ngay trước đó, nhưng tháng này có đơn trở lại.
    --count_lines_L4M=0 tương đương với tháng ngay trước đó Lost, count_lines_L3M<>0 tương đương với tháng này có đơn.
    when count_lines_L4M=0 and count_lines_L3M=0 then 'Inactive'
    --Store ở trạng thái Inactive khi tháng trước ở trạng thái Lost và tháng này cũng không có đơn.
    when count_lines_L3M>0 then 'Existing'
    when count_lines_L3M=0 then 'Lost'
    --Nếu chỉ có điều kiện 3 tháng liên tiếp không có đơn thì trạng thái Lost sẽ bị xác định sai vì nếu tháng thứ liên tiếp vẫn không có đơn thì store  này lại được tính vào trạng thái Lost một lần nữa; trong khi trên thực tế thì nó đã Lost ở tháng ngay trước đó rồi. Vì vậy, cần lọc các tháng có 4 tháng liên tiếp không có đơn trước. Đó là lý do có bước lọc Inactive ở câu lệnh case when bước trước đó.
    else 'Undefined'
  end as customer_status
from calculate_L3M
order by 1,2
)

,count_full_year as
(select
  order_month
  ,count (case when customer_status = 'New' then store_number end) as count_new_customer
  ,count (case when customer_status = 'Lost' then store_number end) as count_lost_customer
  ,count (case when customer_status = 'Returning' then store_number end) as count_returning_customer
  ,count (case when customer_status not in ('Lost','Inactive')  then store_number end) as count_active_customer
from store_by_month_final
group by 1
order by 1
)

select *
from count_full_year
where order_month between '2021-01-01' and '2022-12-31'