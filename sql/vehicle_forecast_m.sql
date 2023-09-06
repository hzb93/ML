insert overwrite table vehicle_forecast_m partition(months='{0}')
select
 a.org_name as area_name
,a.province
,a.city
,a.dealer_code
,a.dealer_shortname
,a.merge_series_code
,a.merge_series_name
,a.sub_merge_series_name
,a.series_code
,a.series_name
,a.model_code
,a.package_code
,a.product_code
,a.product_id
,a.trim_code
,a.trim_color
,a.is_replaced_series
,nvl(b.sales_volume,0) as sales_volume
,nvl(c.order_cnt,0) as order_cnt
,current_date() as update_date
from dealer_mtoc_all a
left join (select dealer_code,product_code,count(distinct vin) as sales_volume
from temp_tt_vs_release_order
WHERE dr='0' and INVOICE_STATUS<>16161002 and record_status=52231001 and substr(report_date,1,7)='{0}'
group by dealer_code,product_code) b
on a.dealer_code=b.dealer_code and a.product_code=b.product_code
left join (select dealer_code,product_id,count(distinct SO_NO_ID) as order_cnt
from temp_tt_sales_order
where BUSINESS_TYPE=14031001 AND CANCEL_DATE IS NULL and substr(if(FIRST_COMMIT_TIME is null,commit_time,first_commit_time),1,7)='{0}'
group by dealer_code,product_id) c
on a.dealer_code=c.dealer_code and a.product_id=c.product_id