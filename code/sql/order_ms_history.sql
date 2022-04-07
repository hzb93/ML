select province,merge_series_code,months,max(update_date) as update_date,nvl(sum(order_cnt),0) as order_cnt
from vehicle_forecast_m
where province is not null and merge_series_code is not null
group by province,merge_series_code,months
order by province,merge_series_code,months