select a.merge_series_code,a.merge_series_name,a.months
,nvl(sum(c.down_cnt),0) as down_cnt
,nvl(sum(d.stockin_cnt),0) as stockin_cnt
,nvl(sum(e.cum_orders),0) as cum_orders
,nvl(sum(a.sales_volume),0) as sales_volume
,nvl(sum(a.order_cnt),0) as order_cnt
,nvl(sum(a.sales_volume),0)/nvl(sum(a.order_cnt),0) as ratio
from (select merge_series_code,merge_series_name,months,product_code,product_id,sum(sales_volume) as sales_volume,sum(order_cnt) as order_cnt
from vehicle_forecast_m where substr(months,1,7)>'2021-01' group by merge_series_code,merge_series_name,months,product_code,product_id) a
left join (SELECT product_code,substr(down_date,1,7) as months,count(DISTINCT vin) AS down_cnt
FROM temp_tm_sl_vehicle
WHERE company_code='GAMCS' AND substr(down_date,1,4)>'2020' and substr(down_date,9,2)<'23'
GROUP BY product_code,substr(down_date,1,7)) c
on a.product_code=c.product_code and a.months=date_format(add_months(concat(c.months,'-01'),1),'yyyy-MM')
left join (SELECT product_code,substr(FIRST_STOCK_IN_DATE,1,7) as months,count(DISTINCT vin) AS stockin_cnt
FROM temp_tm_sl_vehicle
WHERE ENTRY_TYPE=13171001 AND substr(FIRST_STOCK_IN_DATE,1,4)>'2020' and substr(FIRST_STOCK_IN_DATE,9,2)<'23'
GROUP BY product_code,substr(FIRST_STOCK_IN_DATE,1,7)) d
on a.product_code=d.product_code and a.months=date_format(add_months(concat(d.months,'-01'),1),'yyyy-MM')
left join(SELECT o.product_id,substr(o.months,1,7) as months,count(DISTINCT SO_NO_ID) AS cum_orders
FROM (select *,if(FIRST_COMMIT_TIME IS NULL,commit_time,first_commit_time) as months from temp_tt_sales_order) o
WHERE o.BUSINESS_TYPE=14031001
AND o.CANCEL_DATE IS NULL
AND substr(o.months,1,4)>'2020' and substr(o.months,9,2)<'23'
GROUP BY o.product_id,substr(o.months,1,7)) e
on a.product_id=e.product_id and a.months=date_format(add_months(concat(e.months,'-01'),1),'yyyy-MM')
group by a.merge_series_code,a.merge_series_name,a.months
order by a.merge_series_code,a.merge_series_name,a.months