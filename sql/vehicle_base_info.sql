insert overwrite table vehicle_base_info
select
 a.product_id
,a.PRODUCT_CODE
,a.PRODUCT_NAME
,b.CONFIG_CODE as package_code
,b.CONFIG_NAME as package_name
,c.MODEL_CODE
,c.MODEL_NAME
,d.SERIES_CODE
,d.SERIES_NAME
,case
when d.series_code in ('KT1A','KT1B','KT1C') then 'KT1A_KT1B_KT1C'
when d.series_code in ('MS1A','MU1C') then 'MS1A_MU1C'
when d.series_code in ('HR1A','HT1C') then 'HR1A_HT1C'
when d.series_code in ('AA1A','AT2C') then 'AA1A_AT2C'
when d.series_code in ('DK1A','DT2D') then 'DK1A_DT2D'
when d.series_code in ('FR2A','FB2C') then 'FR2A_FB2C'
when d.series_code in ('JS1A','JU1E','JU2A','JB2A') then 'JS1A_JU1E_JU2A_JB2A'
else d.series_code end as merge_series_code
,case
when d.series_code in ('KT1A','KT1B','KT1C') then '传祺M6GM6M6 PRO'
when d.series_code in ('MS1A','MU1C') then '传祺M8GM8'
when d.series_code in ('HR1A','HT1C') then '传祺GS3GS3 POWER'
when d.series_code in ('AA1A','AT2C') then '传祺GS4新GS4'
when d.series_code in ('DK1A','DT2D') then '传祺GS5GS4 PLUS'
when d.series_code in ('FR2A','FB2C') then '传祺GA4GA4 PLUS'
when d.series_code in ('JS1A','JU1E','JU2A','JB2A') then '传祺GS8GS8S第二代GS8'
else d.series_name end as merge_series_name
,case
when d.series_code='MU1C' and substr(a.product_name,6,2)='领秀' then '传祺M8GM8领秀'
when d.series_code in ('KT1A','KT1B','KT1C') then '传祺M6GM6M6 PRO'
when d.series_code in ('MS1A','MU1C') then '传祺M8GM8'
when d.series_code in ('HR1A','HT1C') then '传祺GS3GS3 POWER'
when d.series_code in ('AA1A','AT2C') then '传祺GS4新GS4'
when d.series_code in ('DK1A','DT2D') then '传祺GS5GS4 PLUS'
when d.series_code in ('FR2A','FB2C') then '传祺GA4GA4 PLUS'
when d.series_code in ('JS1A','JU1E','JU2A','JB2A') then '传祺GS8GS8S第二代GS8'
else d.series_name end as sub_merge_series_name
,a.market_price,a.trim_code,a.trim_color
,case when a.IS_STOPPROD=10041001 then 1 else 0 end as is_tingchan_product
,case when series_code in ('B1A','BK1C','DG1B','EL1A','FE1A','FE1B','FJ1B','GN1A','GN1B','HP1A','HP1C','CG1A') then 1 else 0 end as is_tingchan_series
,case when series_code in ('KT1A','KT1B','MS1A','HR1A','AA1A','DK1A','FR2A','JS1A','JU1E') then 1 else 0 end as is_replaced_series
from temp_tm_vs_product a
left join temp_tm_package b
on a.PACKAGE_ID=b.PACKAGE_ID
left join temp_tm_model c
on a.MODEL_ID=c.MODEL_ID
left join temp_tm_series d
on a.SERIES_ID=d.SERIES_ID