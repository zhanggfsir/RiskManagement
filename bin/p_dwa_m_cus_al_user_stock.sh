#!/bin/bash
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dwa_m_cus_al_user_stock.sh
# *功能描述     --%@COMMENT:        离网用户月模型
# *执行周期     --%@PERIOD:         M
# *参数         --%@PARAM:          帐期 YYYYMM
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-06-27
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:
# *修改记录     --%@MODIFY:         2019-06-27
# *来源表       --%@FROM:           ubd_risk_serv.dwa_m_cus_al_user_stock
# *来源表       --%@FROM:           zba_dwa.dwa_v_m_cus_al_user_info 
# *目标表       --%@TO:             ubd_risk_serv.dwa_m_cus_al_user_stock
###############################################################################
# 调用方法: bash p_dwa_m_cus_al_user_stock.sh yyyyMM 
###############################################################################
set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"

##***************************************************************************
#参数说明：该shell模板调用时需传入2个参数：$1为账期（yyyymm）$2为省份(011)
#例如：调用方法：./p_dm_v_m_cus_is_blacklist.sh 201906 011
##***************************************************************************
##加工脚本输入参数
v_date=$1
v_prov_099=099
v_env=$2
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
echo $v_pkg
echo $database
echo $cfg_account
echo $cfg_mas_tb
echo $cfg_sla_tb
v_month=`echo $v_date | cut -c 1-6`
v_month01=${v_month}01
v_part=`echo $((${v_month}%2))`
v_3month=`date +%Y%m -d "$v_month01 -3 months"`
v_1month=`date +%Y%m -d "$v_month01 -1 months"`
v_last_date=$(get_last_day $v_month)
echo $v_date $v_month  $v_part

v_shellname=`basename $0` >>/dev/null
echo "v_shellname1="$v_shellname
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
echo "v_shellname2="$v_shellname

##***************************************************************
##日志文件定义，确定日志文件存放的位置及日志文件名祿
##命名方式为：shell名称_账期_省分_系统时间log
##例如：p_dwa_s_m_acc_al_charge_201310_079_20131127172425.log
##规范：过程名统一小写
##***************************************************************
v_logfile=$(log_file $v_shellname $1 $v_prov_099)
#修改




##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dwa_m_cus_al_user_stock
v_tablename=dwa_m_cus_al_user_stock
#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)

isDependsuccess1=$(is_depend_success $v_date $v_prov_099 dwa_v_m_cus_al_user_info)
#v_depend_procdate4
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】


#判断前置依赖


#if [ 1 -eq 1 ]; then
if [ $isDependsuccess1 -eq 1 ]; then
#定义sql字符?*

v_sql="alter table dwa_m_cus_al_user_stock drop partition (part_id='"$v_month"');"
hive -e "
use $database;
$v_sql
;"

#定义sql字符?
v_sql="insert overwrite table dwa_m_cus_al_user_stock partition(part_id = '"$v_month"')
select
device_number_md5,
user_id,
cust_id,
is_innet,
innet_date,
close_date,
prov_id
from 
(select
row_number() over (PARTITION BY device_number_md5 ORDER BY is_innet desc) sn, 
device_number_md5,
user_id,
cust_id,
is_innet,
innet_date,
close_date,
prov_id
from 
(select 
device_number_md5,
user_id,
cust_id,
is_innet,
innet_date,
close_date,
prov_id
from
$database.dwa_m_cus_al_user_stock
where 
part_id='"$v_1month"'
union all
select
upper(device_number_en) device_number_md5,
user_id,
cust_id,
is_innet,
innet_date,
close_date,
prov_id
from  
zba_dwa.dwa_v_m_cus_al_user_info 
where 
month_id='"$v_month"') t
group by
device_number_md5,
user_id,
cust_id,
is_innet,
innet_date,
close_date,
prov_id
)t1
where is_innet=0 and t1.sn=1;"

#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dwa_m_cus_al_user_stock_$v_month;
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
$v_sql;
" 2>&1 |tee $v_logfile >>/dev/null


#删除3月前的数据 
v_sql="alter table dwa_m_cus_al_user_stock drop partition (part_id='"$v_3month"');"
hive -e "
use $database;
$v_sql
;"


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
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_retinfo $v_retcode $v_rowline)
#****************************************