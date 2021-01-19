#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_serv_d_cus_risk_al_returner
# *功能描述     --%@COMMENT:      	三要素数据加工
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        张广峰
# *创建时间     --%@CREATED_TIME:   20200303
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *修改记录     --%@MODIFY:       	
# *来源表       --%@FROM:           ubd_risk_serv.serv_d_cus_risk_fence_info
# *来源表       --%@FROM:           ubd_x_dim.dim_xzqh_final
# *目标表       --%@TO:             ubd_risk_serv.serv_d_cus_risk_al_returner
###############################################################################
# 调用方法: sh -x p_serv_d_cus_risk_al_returner.sh 20200302 release 
###############################################################################

set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"

v_date=$1
v_prov=099
v_prov_099=099
v_env=$2
#常量定义
ip=10.244.16.99
username=sjzl
password=sjzl
local_path=/tmp/marketing/

if [ -z $v_env ]; then 
	v_env=develop
fi

if  [ $v_env = 'develop' ]; then
	v_pkg=p_ubd_risk_test
	database=ubd_risk_test
	cfg_mas_tb=DIM_SERV_DATA_INTERACT_PRO_319_TEST
	cfg_sla_tb=DIM_SERV_DATA_INTERACT_PRO_SLAVE_319_TEST
	cfg_account=DIM_SERV_DATA_WEBSERVICE_TEST
	hbaseDB=ubd_test
elif [ $v_env = 'release' ]; then
	v_pkg=p_ubd_risk_serv
	database=ubd_risk_serv
	cfg_mas_tb=DIM_SERV_DATA_INTERACT_PRO_319
	cfg_sla_tb=DIM_SERV_DATA_INTERACT_PRO_SLAVE_319
	cfg_account=DIM_SERV_DATA_WEBSERVICE
	hbaseDB=ubd_master
fi

v_month_id=${v_date:0:6}
v_day_id=${v_date:6:2}

v_shellname=`basename $0` >>/dev/null
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
 
v_logfile=$(log_file $v_shellname $1 $v_prov_099)

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_serv_d_cus_risk_al_returner
v_tablename=serv_d_cus_risk_al_returner
#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename)



isDependsuccess=$(is_depend_success_no_prov_tour2risk $v_date serv_d_cus_tour_al_location_stay_filter)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate1=$v_date
else
v_depend_procdate1=$(new_date_tour2risk $v_prov_099 serv_d_cus_tour_al_location_stay_filter)
v_depend_procdate1=$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename $v_depend_proc1 $v_depend_procdate1)

v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`

# 1日前
v_date1=`date -d "${v_depend_procdate1} 1 day ago" +"%Y%m%d"`
v_month_id1=${v_date1:0:6}
v_day_id1=${v_date1:6:2}

# 14日前
v_date14=`date -d "${v_date1} 14 day ago" +"%Y%m%d"`
v_month_id14=${v_date14:0:6}
v_day_id14=${v_date14:6:2}


#判断前置依赖
v_cnt1=$(is_depend_success_no_prov_tour2risk $v_depend_procdate1 serv_d_cus_tour_al_location_stay_filter)

#if [ 1 -eq 1 ]; then
if [ $v_cnt1 -eq 1 ]; then

#数据入库
v_sql1="
LOAD DATA INPATH '/user/ubd_test_sdyx/test_newEngine/rule_${v_date}.txt' OVERWRITE INTO TABLE ubd_risk_serv.serv_d_cus_risk_fence_info PARTITION (month_id=${v_month_id}, day_id=${v_day_id}, prov_id=${v_prov_099});"

hive -e "
$v_sql1;
MSCK REPAIR TABLE ubd_risk_serv.serv_d_cus_risk_fence_info;
"
#先删除分区
v_sql="alter table ubd_risk_serv.serv_d_cus_risk_al_returner drop partition (month_id=${v_month_id}, day_id=${v_day_id}, prov_id=${v_prov_099});"
hive -e "
use $database;
$v_sql
;" 


if  [ $v_month_id14 -eq $v_month_id1 ]; then
# 同月
v_sql2="insert overwrite table ubd_risk_serv.serv_d_cus_risk_al_returner partition (month_id=${v_month_id}, day_id=${v_day_id}, prov_id=${v_prov_099})
select t1.fence_id, t1.activity_id, t1.device_number,
  split(split(t2.city_list, '#')[0], '_')[0] top1_prov,
  split(split(t2.city_list, '#')[0], '_')[1] top1_prov_name,
  split(split(t2.city_list, '#')[0], '_')[2] top1_area,
  split(split(t2.city_list, '#')[0], '_')[3] top1_area_name,
  split(split(t2.city_list, '#')[1], '_')[0] top2_prov,
  split(split(t2.city_list, '#')[1], '_')[1] top2_prov_name,
  split(split(t2.city_list, '#')[1], '_')[2] top2_area,
  split(split(t2.city_list, '#')[1], '_')[3] top2_area_name,
  split(split(t2.city_list, '#')[2], '_')[0] top3_prov,
  split(split(t2.city_list, '#')[2], '_')[1] top3_prov_name,
  split(split(t2.city_list, '#')[2], '_')[2] top3_area,
  split(split(t2.city_list, '#')[2], '_')[3] top3_area_name,
  split(split(t2.city_list, '#')[3], '_')[0] top4_prov,
  split(split(t2.city_list, '#')[3], '_')[1] top4_prov_name,
  split(split(t2.city_list, '#')[3], '_')[2] top4_area,
  split(split(t2.city_list, '#')[3], '_')[3] top4_area_name,
  split(split(t2.city_list, '#')[4], '_')[0] top5_prov,
  split(split(t2.city_list, '#')[4], '_')[1] top5_prov_name,
  split(split(t2.city_list, '#')[4], '_')[2] top5_area,
  split(split(t2.city_list, '#')[4], '_')[3] top5_area_name
from (
  select a.activity_id, a.fence_id, a.prov, a.area, b.device_number, b.time_diff 
  from (
    select activity_id, fence_id, prov, area from ubd_risk_serv.serv_d_cus_risk_fence_info where month_id=${v_month_id} and day_id=${v_day_id}) a
    inner join (select device_number, prov, area, sum(time_diff) as time_diff from ubd_serv_tour.serv_d_cus_tour_al_location_stay_filter 
      where month_id=${v_month_id} and day_id=${v_day_id} group by device_number, prov, area having sum(time_diff)>360) b on a.prov=b.prov and a.area=b.area ) t1 
left join (
  select d.device_number, concat_ws('#',collect_set(concat(d.prov, '_', d.prov_name, '_', d.area, '_',  d.area_name))) city_list from
  (select device_number, prov, prov_name, area, area_name, time_diff, row_number() over(partition by device_number order by time_diff desc) sn
  from (
    select c1.device_number, c1.prov, c2.prov_name, c1.area, c2.area_name, max(c1.time_diff) time_diff
    from (
      select device_number, prov, area, day_id, sum(time_diff) time_diff
      from ubd_serv_tour.serv_d_cus_tour_al_location_stay_filter
      where month_id=${v_month_id14} and day_id>${v_day_id14} and  day_id<=${v_day_id1}
      group by device_number, prov, area, day_id having sum(time_diff)>240 ) c1
      left join (select prov_id prov, prov_desc prov_name, area_id area, area_desc area_name from ubd_x_dim.dim_xzqh_final) c2 on c1.prov=c2.prov and c1.area=c2.area
    group by c1.device_number, c1.prov, c2.prov_name, c1.area, c2.area_name) c
  ) d
  group by d.device_number
) t2 on t1.device_number=t2.device_number
where instr(t2.city_list, t1.area)=0
;"
else
# 异月
#定义sql字符
v_sql2="insert overwrite table ubd_risk_serv.serv_d_cus_risk_al_returner partition (month_id=${v_month_id}, day_id=${v_day_id}, prov_id=${v_prov_099})
select t1.fence_id, t1.activity_id, t1.device_number,
  split(split(t2.city_list, '#')[0], '_')[0] top1_prov,
  split(split(t2.city_list, '#')[0], '_')[1] top1_prov_name,
  split(split(t2.city_list, '#')[0], '_')[2] top1_area,
  split(split(t2.city_list, '#')[0], '_')[3] top1_area_name,
  split(split(t2.city_list, '#')[1], '_')[0] top2_prov,
  split(split(t2.city_list, '#')[1], '_')[1] top2_prov_name,
  split(split(t2.city_list, '#')[1], '_')[2] top2_area,
  split(split(t2.city_list, '#')[1], '_')[3] top2_area_name,
  split(split(t2.city_list, '#')[2], '_')[0] top3_prov,
  split(split(t2.city_list, '#')[2], '_')[1] top3_prov_name,
  split(split(t2.city_list, '#')[2], '_')[2] top3_area,
  split(split(t2.city_list, '#')[2], '_')[3] top3_area_name,
  split(split(t2.city_list, '#')[3], '_')[0] top4_prov,
  split(split(t2.city_list, '#')[3], '_')[1] top4_prov_name,
  split(split(t2.city_list, '#')[3], '_')[2] top4_area,
  split(split(t2.city_list, '#')[3], '_')[3] top4_area_name,
  split(split(t2.city_list, '#')[4], '_')[0] top5_prov,
  split(split(t2.city_list, '#')[4], '_')[1] top5_prov_name,
  split(split(t2.city_list, '#')[4], '_')[2] top5_area,
  split(split(t2.city_list, '#')[4], '_')[3] top5_area_name
from (
  select a.activity_id, a.fence_id, a.prov, a.area, b.device_number, b.time_diff 
  from (
    select activity_id, fence_id, prov, area from ubd_risk_serv.serv_d_cus_risk_fence_info where month_id=${v_month_id} and day_id=${v_day_id}) a
    inner join (select device_number, prov, area, sum(time_diff) as time_diff from ubd_serv_tour.serv_d_cus_tour_al_location_stay_filter 
      where month_id=${v_month_id} and day_id=${v_day_id} group by device_number, prov, area having sum(time_diff)>360) b on a.prov=b.prov and a.area=b.area ) t1 
left join (
  select d.device_number, concat_ws('#',collect_set(concat(d.prov, '_', d.prov_name, '_', d.area, '_',  d.area_name))) city_list from
  (select device_number, prov, prov_name, area, area_name, time_diff, row_number() over(partition by device_number order by time_diff desc) sn
  from (
    select c1.device_number, c1.prov, c2.prov_name, c1.area, c2.area_name, max(c1.time_diff) time_diff
    from (
      select device_number, prov, area, day_id, sum(time_diff) time_diff
      from ubd_serv_tour.serv_d_cus_tour_al_location_stay_filter
      where ((month_id=${v_month_id14} and day_id>${v_day_id14}) or (month_id=${v_month_id1} and day_id<=${v_day_id1}))
      group by device_number, prov, area, day_id having sum(time_diff)>240 ) c1
      left join (select prov_id prov, prov_desc prov_name, area_id area, area_desc area_name from ubd_x_dim.dim_xzqh_final) c2 on c1.prov=c2.prov and c1.area=c2.area
    group by c1.device_number, c1.prov, c2.prov_name, c1.area, c2.area_name) c
  ) d
  group by d.device_number
) t2 on t1.device_number=t2.device_number
where instr(t2.city_list, t1.area)=0 
;"

fi

#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_serv_d_cus_risk_al_returner_$v_month_$v_day;
set mapred.job.priority=HIGH;
set hive.optimize.cp=true;
set hive.optimize.pruner=true;
set hive.groupby.skewindata=true;
set mapred.output.compression.type=BLOCK;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapred.max.split.size=100000000;
set mapreduce.job.queuename=ia_serv;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.auto.convert.join=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true; 
set hive.merge.mapredfiles=true; 
set hive.merge.size.per.task=256000000; 
set hive.merge.smallfiles.avgsize=1073741824; 
$v_sql2;
" 2>&1 |tee $v_logfile >>/dev/null 

#获取过程执行情况（通过p_pub_func_analyze.sh中的isExeSuccess方法/函数进行判断）
v_result=$(is_exe_success $v_logfile) >>/dev/null
if  [ $v_result -eq 1 ]; then

#数据拉本地 合并小文件
hadoop fs -text /user/ubd_master/ubd_risk_serv.db/serv_d_cus_risk_al_returner/month_id=${v_month_id}/day_id=${v_day_id}/prov_id=${v_prov_099}/* > ${local_path}returner_${v_month_id}${v_day_id}.txt

#数据放于FTP

ftp -n<<ENDFTP  > /dev/null 2>&1
open 10.244.16.99
user sjzl sjzl
lcd ${local_path}
prompt
bin
put returner_${v_month_id}${v_day_id}.txt
close
bye
ENDFTP

rm ${local_path}*

v_retcode=SUCCESS
v_retinfo=结束
#获取结果记录行数（通过p_pub_func_analyze.sh中的getRowline_spare方法/函数进行判断）
v_rowline=$(get_row_line_spare $v_logfile) >>/dev/null
else
v_retcode=FAIL
#获取执行错误原因（通过p_pub_func_analyze.sh中的getFailedInfo方法/函数进行判断）
v_retinfo=$(get_failed_info $v_logfile)
v_retinfo=${v_retinfo//\'/\"}
v_retinfo=${v_retinfo// /|}
echo $v_retcode
fi

else
v_retcode=WAIT
v_retinfo=等待
fi
echo $v_retcode

#更新日志
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_retinfo $v_retcode $v_rowline)
#****************************************


