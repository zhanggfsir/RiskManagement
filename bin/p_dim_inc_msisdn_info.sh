#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dim_inc_msisdn_info.sh
# *功能描述     --%@COMMENT:      	dim_msisdn_sha256_md5  pt列 手机号查明文
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        张广峰
# *创建时间     --%@CREATED_TIME:   2019-10-16
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *修改记录     --%@MODIFY:       	2019-10-16 | 张广峰 | 
# *来源表       --%@FROM:           ubd_dm.dwa_v_d_cus_np_turn_info
# *来源表       --%@FROM:           ubd_risk_serv.dim_all_msisdn
# *来源表       --%@FROM:           ubd_risk_serv.dim_all_msisdn_info
# *来源表       --%@FROM:           ubd_risk_serv.dim_all_msisdn_info_tmp
# *目标表       --%@TO:             ubd_risk_serv.dim_inc_msisdn_info
###############################################################################
# 调用方法: bash p_dim_inc_msisdn_info.sh 20190417 release
###############################################################################

set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"

##***************************************************************************
#参数说明：该shell模板调用时需传入2个参数：$1为账期（yyyymm）$2为省分（xxx）
#例如：调用方法：./p_test.sh 20170401 010
##***************************************************************************
##加工脚本输入参数
v_date=$1
v_prov=099
v_prov_099=099
v_env=$2
if [ -z $v_env ]; then 
	v_env=develop
fi

if  [ $v_env = 'develop' ]; then
	v_pkg=p_ubd_risk_serv
	database=ubd_risk_serv
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
echo $v_pkg
echo $database
echo $cfg_account
echo $cfg_mas_tb
echo $cfg_sla_tb
v_month=`echo $v_date | cut -c 1-6`
v_day=`echo $v_date | cut -c 7-8`
v_part=`echo $((${v_month}%2))`
v_last_day1=`date +%Y%m%d -d "$v_date -1 days"`
v_month1=`echo $v_last_day1 | cut -c 1-6`
v_day1=`echo $v_last_day1 | cut -c 7-8`

v_last_day7=`date +%Y%m%d -d "$v_date -7 days"`
v_7month=`echo $v_last_day7 | cut -c 1-6`
v_7day=`echo $v_last_day7 | cut -c 7-8`
echo $v_date $v_month $v_day $v_part

v_shellname=`basename $0` >>/dev/null
echo "v_shellname1="$v_shellname
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
echo "v_shellname2="$v_shellname
 
##***************************************************************
##日志文件定义，确定日志文件存放的位置及日志文件名祿
##命名方式为：shell名称_账期_省分_系统时间憿log
##例如：p_dwa_s_m_acc_al_charge_201310_079_20131127172425.log
##规范：过程名统一小写
##***************************************************************
v_logfile=$(log_file $v_shellname $1 $v_prov)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dim_inc_msisdn_info
v_tablename=dim_inc_msisdn_info
#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)

##判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
#isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_np_turn_info)
#if [ $isDependsuccess -eq 1 ]; then
#v_depend_procdate1=$v_date
#else
##获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
##使用的是 new_date 是部分省的 还有省份
#v_depend_procdate1=$(new_date $v_prov_099 dwa_v_d_cus_np_turn_info)
#echo "v_depend_procdate1="$v_depend_procdate1
#fi
##记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
#$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename $v_depend_proc1 $v_depend_procdate1)


v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`

#判断前置依赖

v_cnt1=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_np_turn_info)
echo "v_cnt1="$v_cnt1

#if [ 1 -eq 1 ]; then
if [ $v_cnt1 -ge 1  ]; then
#定义sql字符?*
v_sql="truncate table ubd_risk_serv.dim_inc_msisdn_info;"
hive -e "
use $database;
$v_sql
;" 

# 仅需要加工一次，得到增量表，没调用 v_sql0 。以后直接跑脚本就OK
v_sql0="insert overwrite table ubd_risk_serv.dim_inc_msisdn_info partition(mac)
select upper(md5(t1.msisdn)) as device_number_md5,t1.msisdn as device_number,nvl(t2.turn_in_dealer,t3.serv_type) serv_type,t3.prov_id,t3.area_id,t2.turn_out_dealer,t2.effect_date,substr(t1.msisdn,1,3) as mac
from 
(select msisdn from ubd_risk_serv.dim_all_msisdn) t1 
left join 
(select device_number,turn_in_dealer,turn_out_dealer,effect_date,device_number_md5 from ubd_dm.dwa_v_d_cus_np_turn_info where part_id=201908 and day_id=28) t2 
on t1.msisdn=t2.device_number 
left join 
(select device_no,serv_type,prov_id,area_id from zba_dim.dim_msisdn_seg_code) t3
on substr(t1.msisdn,1,7)=t3.device_no;
"

#定义sql字符?
#每日得到增量表 较上一日的 增量
v_sql1="insert overwrite table ubd_risk_serv.dim_inc_msisdn_info partition(mac)
select upper(t1.device_number_md5) as device_number_md5,t1.device_number,nvl(t1.turn_in_dealer,t3.serv_type) as serv_type,t3.prov_id,t3.area_id,t1.turn_out_dealer,t1.effect_date,substr(t1.device_number,1,3) as mac
from
(select device_number,turn_out_dealer,turn_in_dealer,effect_date,device_number_md5 from ubd_dm.dwa_v_d_cus_np_turn_info where part_id='$v_month' and day_id='$v_day') t1 
left join
(select device_number,turn_out_dealer,turn_in_dealer,effect_date,device_number_md5 from ubd_dm.dwa_v_d_cus_np_turn_info where part_id='$v_month1' and day_id='$v_day1') t2 on t1.device_number=t2.device_number 
left join
(select device_no,serv_type,prov_id,area_id from zba_dim.dim_msisdn_seg_code) t3
on substr(t1.device_number,1,7)=t3.device_no  where t2.device_number is null; "


#增量表和全量表得到中间表， 中间表入全量表， 清空中间表

v_sql2="insert overwrite table ubd_risk_serv.dim_all_msisdn_info_tmp partition(mac)
select t1.device_number_md5,t1.device_number,nvl(t2.serv_type,t1.serv_type),nvl(t2.prov_id,t1.prov_id),nvl(t2.area_id,t1.area_id),nvl(t2.turn_out_dealer,t1.turn_out_dealer),nvl(t2.effect_date,t1.effect_date),t1.mac
from
(select device_number_md5,device_number,serv_type,prov_id,area_id,turn_out_dealer,effect_date,mac from ubd_risk_serv.dim_all_msisdn_info) t1
left join 
(select device_number_md5,device_number,serv_type,prov_id,area_id,turn_out_dealer,effect_date,mac from ubd_risk_serv.dim_inc_msisdn_info) t2
on t1.device_number=t2.device_number and t1.mac=t2.mac;"

#中间表入全量表
v_sql3="insert overwrite table ubd_risk_serv.dim_all_msisdn_info partition(mac)
select device_number_md5,device_number,serv_type,prov_id,area_id,turn_out_dealer,effect_date,mac from ubd_risk_serv.dim_all_msisdn_info_tmp ;"

#清空中间表
v_sql4="truncate TABLE  ubd_risk_serv.dim_all_msisdn_info_tmp;"

#触发入库流程，增量表 ubd_risk_serv.dim_inc_msisdn_info 入库


#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dim_inc_msisdn_info_$v_month_$v_day;
set mapred.job.priority=HIGH;
set hive.optimize.cp=true;
set hive.optimize.pruner=true;
set hive.groupby.skewindata=true;
set hive.exec.compress.output=false;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
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
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
$v_sql1;
$v_sql2;
$v_sql3;
$v_sql4;
" 2>&1 |tee $v_logfile >>/dev/null 

#删除7天前的数据 
#v_sql="alter table dim_inc_msisdn_info drop partition (month_id='"$v_7month"',day_id='"$v_7day"');"
#hive -e "
#use $database;
#$v_sql
#;"

#获取过程执行情况（通过p_pub_func_analyze.sh中的isExeSuccess方法/函数进行判断）
v_result=$(is_exe_success $v_logfile) >>/dev/null
if  [ $v_result -eq 1 ]; then
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
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_retinfo $v_retcode $v_rowline)
#****************************************
