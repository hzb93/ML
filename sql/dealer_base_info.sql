insert overwrite table dealer_base_info
    select
       a.dealer_code,
       a.dealer_shortname,
       a.dealer_name,
       a.open_date,
       a.close_time,
       case when a.DEALER_STATUS=10111002 then '正常' else '退网' end as dealer_state,
       c.org_name,
       case
       when a.dealer_code in ('DGXA040','DGXM010') then '广西壮族自治区'
       when a.dealer_code in ('DNXA050') then '宁夏回族自治区'
       when a.dealer_code in ('DNMD030','DNMG020') then '内蒙古自治区'
       when a.dealer_code in ('DXJL020') then '新疆维吾尔自治区'
       when a.dealer_code in ('DBJA170') then '北京市'
       else d.region_name end as province,
       case
       when a.dealer_code in ('DXJL020') then '哈密地区'
       else e.region_name end as city,
       f.region_name as county
From (select *from temp_tm_dealer_basicinfo
where IS_MAJOR=10041002
and substr(DEALER_CODE,1,1)='D'
and DEALER_CODE NOT LIKE 'GA%'
and DEALER_CODE NOT LIKE '000%'
and DEALER_CODE NOT LIKE 'DAAA%'
and DEALER_CODE NOT LIKE 'DBBB%'
and DEALER_CODE NOT LIKE '60%'
and DEALER_CODE NOT LIKE 'G0%'
and DEALER_CODE NOT LIKE 'DCK0%') a
left join temp_tm_dealerchannel b
on a.DEALER_CODE = b.DEALER_COMPANY_ID
left join (select *from temp_tm_com_organization where org_type=16801003) c
on b.SALES_AREA = c.org_code
left join (select *from temp_tm_region where region_type = 10001001) d
on a.province=d.region_id
left join (select *from temp_tm_region where region_type = 10001002) e
on a.city=e.region_id
left join (select *from temp_tm_region where region_type = 10001003) f
on a.county=f.region_id