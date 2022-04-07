###程序名称：   insert_table_${table_name}.sh 
###程序用途：   生成权益清单（每天跑）
##数据源表##： bss.rpt_comm_cm_msdisc                               --销售品资料表
############： scb_khjz.drep_rzk_mbyh_list                          --
############： bss.rpt_comm_ba_msdisc/bss.rpt_comm_ba_msdisc_hist   --销售品订单受理统计资料
############： bss.staff/dgdm.dim_sm_chnlorg/bss.offer          --相关维表
############：  
###目标表  ：  scb_khjz.zch_fxb_cxb_data_detail_d      --清单明细  日分区【hive】   sd_scb_dgdw.zch_fxb_cxb_data_detail_d   --清单明细  日分区【行云】
############   scb_khjz.zch_fxb_cxb_data_tj_d          --统计结果  日分区【hive】   sd_scb_dgdw.zch_fxb_cxb_data_tj_d       --统计结果  日分区【行云】
############
############
###开发日期：   2019-10-17
###开发人  ：   huangqun
###执行方式：   
###备注说明：   shell执行后面参数分别是etl里面传过来的
###sh /home/dg_scb/hq/sh/insert_table_dmps_zch_fxb_cxb_data_d.sh -a ${op_time_1} -u ${sys.epid} -v ${sys.jobid}




#!/bin/sh
source ~/.bash_profile

while  getopts  " a:v:u: "  arg #选项后面的冒号表示该选项需要参数
do
         case  $arg  in
             a)
                echo  " op_time_1:$OPTARG "  #参数存在$OPTARG中
                op_time_1=$OPTARG   #今天日期
                ;;
             v)
                echo  " jobid:$OPTARG "  #参数存在$OPTARG中
                jobid=$OPTARG
                ;;
             u)
                echo  " epid:$OPTARG "
                epid=$OPTARG
                ;;
              ? )  #当有不认识的选项的时候arg为 ?
            echo  " unkonw argument "
        exit  1
        ;;
        esac
done

#账期月
op_time_m_1=`expr substr $op_time_1 1 6`

#取数的数据分区日期，选系统当前时间前1天日期                      
yesterday_date=$(date -d -1day +%Y%m%d) 
          
#echo "昨日日期为: ${yesterday_date}"

echo "传入参数情况："
echo "**************************************："
echo "本次跑数账期为:$op_time_1"
echo "跑数账期归属月份为:$op_time_m_1"
echo "昨天日期为:$yesterday_date"
echo "本次跑数结果宽表分区日期为:$op_time_1"


start_time=`date "+%Y-%m-%d %H:%M:%S"`

echo "程序执行开始时间:$start_time"


hive -e"
use scb_khjz;

set hive.exec.compress.output=false;
set mapreduce.job.queuename=dg_scb;

--在用产品资料表,是否为当月开通优惠
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_01;
create table if not exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_01 as
select distinct
a.acc_nbr,
prod_offer_id,
case when b.acc_nbr is not null then 1 else 0 end as is_mbyh,
case when substr(open_date,1,6)='${op_time_m_1}' then 1 else 0 end as is_dy 
from 
(
select distinct 
acc_nbr,
regexp_replace(substr(open_date,1,10),'-','') as open_date,
prod_offer_id
from bss.rpt_comm_cm_msdisc where receive_day='${yesterday_date}'  
and prod_id in (3204,3205) 
and regexp_replace(substr(open_date,1,10),'-','')>='20190801'  
and prod_offer_id in (500047085,500047086,500044119,500044118,500048076)
) a 
left join 
(
select distinct acc_nbr
from scb_khjz.drep_rzk_mbyh_list 
where report_name='放心包、畅享包日报'
) b on a.acc_nbr=b.acc_nbr
;



--2产品受理表,查看指定产品的办理记录
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_02;
create table if not exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_02 as
select t2.* 
from(
      select t1.* 
            ,row_number() over(partition by t1.acc_nbr,t1.prod_offer_code,t1.subs_stat_date order by t1.salestaff_id desc) as n1 
      from
      (
            select a.cust_id,a.serv_id,a.acc_nbr,a.prod_id,a.prod_offer_id,a.salestaff_id
                  ,translate(substr(a.subs_stat_date,1,10),'-','') as subs_stat_date
                  ,b.prod_offer_code
            from (
                  select acc_nbr,acc_nbr2,serv_id,cust_id,prod_id,prod_offer_id,subs_stat_date,salestaff_id
                  from bss.rpt_comm_ba_msdisc
                  where receive_day='${yesterday_date}'  --每次取昨天账期数据
                    and subs_stat = '301200'             --已归档
                    and subs_stat_reason <> '1200'       --撤单
                    and action_id in (1292)              --销售品订购
                    and translate(substr(subs_stat_date,1,10),'-','')>='20190801'
                  )a
            join (
                  select distinct offer_id,offer_name,prod_offer_code
                  from bss.offer 
                  where receive_day='${yesterday_date}'   
                  and prod_offer_code in ('YD4G02-436-1-1','YD4G02-436-1-2','YD4G02-436-1-3','YD4G02-420-1-1','YD4G02-420-1-2') --放心包畅享包
                  )b 
            on a.prod_offer_id=b.offer_id 
      union all 
            select a.cust_id,a.serv_id,a.acc_nbr,a.prod_id,a.prod_offer_id,a.salestaff_id
                  ,translate(substr(a.subs_stat_date,1,10),'-','') as subs_stat_date
                  ,b.prod_offer_code
            from (
                  select acc_nbr,acc_nbr2,serv_id,cust_id,prod_id,prod_offer_id,subs_stat_date,salestaff_id
                  from bss.rpt_comm_ba_msdisc_hist
                  where 1=1
                    and subs_stat = '301200'             --已归档
                    and subs_stat_reason <> '1200'       --撤单
                    and action_id in (1292)              --销售品订购
                    and translate(substr(subs_stat_date,1,10),'-','')>='20190801'  
                  )a
            join (
                  select distinct offer_id,offer_name,prod_offer_code
                  from bss.offer 
                  where receive_day='${yesterday_date}'   --每次取昨天账期数据
                  and prod_offer_code in ('YD4G02-436-1-1','YD4G02-436-1-2','YD4G02-436-1-3','YD4G02-420-1-1','YD4G02-420-1-2')  --放心包畅享包
                    )b 
            on a.prod_offer_id=b.offer_id 
     )t1
)t2  where t2.n1=1
;

--3
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_03;
create table if not exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_03 as
select distinct 
a.acc_nbr,
a.prod_offer_id,
a.is_mbyh,
a.is_dy,
b.salestaff_id
from scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_01 a 
left join scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_02 b 
on a.acc_nbr=b.acc_nbr and a.prod_offer_id=b.prod_offer_id
;

--输出表
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_04;                      
create table if not exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_04 as
select distinct p2.acc_nbr,p2.is_mbyh,p2.is_dy
      ,case when p4.qd_dl is null or p4.qd_dl='' then '电子渠道' 
            when p4.qd_dl='营业厅' then '营业渠道' else p4.qd_dl end as lz_big_qd
      ,case when p4.qd_xl is null or p4.qd_xl='' then '其他' else p4.qd_xl end as lz_small_qd
      ,case when p4.sales_man_name is null or p4.sales_man_name='' then '其他' else p4.sales_man_name end as lz_name
      ,case when p4.n_branch_id_new like '%镇%' then substr(p4.n_branch_id_new,1,locate('镇',p4.n_branch_id_new)-1)
            when p4.n_branch_id_new like '%分%' then substr(p4.n_branch_id_new,1,locate('分',p4.n_branch_id_new)-1) else '其他' end as lz_branch_name
from scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_03 p2
left join 
( 
select cast(staff_id as string) as staff_id,staff_code from bss.staff where receive_day='${yesterday_date}'
) p3 on p2.salestaff_id=p3.staff_id
left join dgdm.dim_sm_chnlorg p4
on p3.staff_code=p4.sales_code
;

--渠道统计
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_05;                       
create table if not exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_05 as
select lz_big_qd,sum(is_dy) as dy_num 
      ,count(1) as all_num 
from scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_04
group by lz_big_qd;


--hive结果表
--数据明细
alter table scb_khjz.zch_fxb_cxb_data_detail_d add if NOT EXISTS partition (receive_day=${op_time_1});
insert overwrite table scb_khjz.zch_fxb_cxb_data_detail_d partition(receive_day=${op_time_1})
select
'${op_time_1}' as acct_day,
acc_nbr,       
is_mbyh,       
is_dy,         
lz_big_qd,     
lz_small_qd,   
lz_name,       
lz_branch_name
from scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_04;

--渠道统计
alter table scb_khjz.zch_fxb_cxb_data_tj_d add if NOT EXISTS partition (receive_day=${op_time_1});
insert overwrite table scb_khjz.zch_fxb_cxb_data_tj_d partition(receive_day=${op_time_1})
select
'${op_time_1}' as acct_day,
lz_big_qd, 
nvl(dy_num,0 ) as dy_num,   
nvl(all_num,0 ) as all_num
from scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_05;


drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_01;
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_02;
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_03;
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_04;
drop table if exists scb_khjz.dtemp_${op_time_1}_zch_day_report_fxb_05;

"


end_time=`date "+%Y-%m-%d %H:%M:%S"`

echo "开始时间:$start_time"
echo "结束时间:$end_time"

###判断表是否成功， 999：错误状态， 3：完成状态 
##if [ $? -ne 0 ];then
##        echo $epid.$jobid.999 > /home/etlsh/workspace/COMM/etl_logs/Monitor_${jobid}_${epid}.mtr
##        echo "失败状态999"
##else
##        echo $epid.$jobid.3 > /home/etlsh/workspace/COMM/etl_logs/Monitor_${jobid}_${epid}.mtr
##         echo "成功状态3"
##fi


##hive存储表结构
##结果明细清单【天表】
##create external table if not exists scb_khjz.zch_fxb_cxb_data_detail_d
##(
##acct_day                string   comment   '账期',
##acc_nbr                 string   comment   '账号',
##is_mbyh                 int      comment   '是否为目标用户',
##is_dy                   int      comment   '是否为当月新增',
##lz_big_qd               string   comment   '揽装渠道大类',
##lz_small_qd             string   comment   '揽装渠道小类',
##lz_name                 string   comment   '揽装人',
##lz_branch_name          string   comment   '揽装镇分'
##)
##partitioned by (receive_day string)  
##row format delimited fields terminated by '^' 
##location 'hdfs://beh/ywzc/scb_khjz/zch_fxb_cxb_data_detail_d';


##结果统计清单【天表】
##create external table if not exists scb_khjz.zch_fxb_cxb_data_tj_d
##(
##acct_day                string   comment   '账期',
##lz_big_qd               string   comment   '揽装渠道',
##dy_num                  bigint   comment   '单移',
##all_num                 bigint   comment   '总数'
##)
##partitioned by (receive_day string)  
##row format delimited fields terminated by '^' 
##location 'hdfs://beh/ywzc/scb_khjz/zch_fxb_cxb_data_tj_d';


##行云建表
##create table sd_scb_dgdw.zch_fxb_cxb_data_detail_d
##(
##acct_day                varchar(20)   comment   '账期',
##acc_nbr                 varchar(20),  comment   '账号',
##is_mbyh                 long          comment   '是否为目标用户',
##is_dy                   long          comment   '是否为当月新增',
##lz_big_qd               varchar(100)  comment   '揽装渠道大类',
##lz_small_qd             varchar(100)  comment   '揽装渠道小类',
##lz_name                 varchar(100)  comment   '揽装人',
##lz_branch_name          varchar(100)  comment   '揽装镇分'
##)
##partitioned by (acct_day);

##结果统计清单【天表】
##create table sd_scb_dgdw.zch_fxb_cxb_data_tj_d
##(
##acct_day                varchar(20)     comment   '账期',
##lz_big_qd               varchar(100)    comment   '揽装渠道',
##dy_num                  long            comment   '单移',
##all_num                 long            comment   '总数'
##)
##partitioned by (acct_day)
