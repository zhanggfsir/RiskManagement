#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:          p_dwa_v_d_cus_jingxun_internet_taxi.sh
# *功能描述     --%@COMMENT:      	精讯网约车中间表
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-07-10
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:
# *来源表       --%@FROM:           ubd_dm.dwa_d_ia_basic_user_app
# *目标表       --%@TO:             ubd_risk_serv.dwa_d_cus_jingxun_internet_taxi
###############################################################################
# 调用方法: bash p_dwa_v_d_cus_jingxun_internet_taxi.sh 20190417
###############################################################################

set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"

##加工脚本输入参数
v_date=$1
v_prov=099
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
v_day=`echo $v_date | cut -c 7-8`
v_last_day30=`date +%Y%m%d -d "$v_date -30 days"`
v_30month=`echo $v_last_day30 | cut -c 1-6`
v_30day=`echo $v_last_day30 | cut -c 7-8`
echo $v_date $v_month $v_day $v_last_day_30

v_shellname=`basename $0` >>/dev/null
echo "v_shellname1="$v_shellname
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
echo "v_shellname2="$v_shellname

##***************************************************************
##日志文件定义，确定日志文件存放的位置及日志文件名祿
##命名方式为：shell名称_账期_省分_系统时间憿log
##规范：过程名统一小写
##***************************************************************
v_logfile=$(log_file $v_shellname $1 $v_prov)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dwa_d_cus_jingxun_internet_taxi
v_tablename=dwa_d_cus_jingxun_internet_taxi
#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_d_ia_basic_user_app)
if [ $isDependsuccess -eq 31 ]; then
    v_depend_procdate1=$v_date
else
    #获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
    #使用的是 new_date 是部分省的 还有省份
    v_depend_procdate1=$(new_date $v_prov_099 dwa_d_ia_basic_user_app)
    echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename $v_depend_proc1 $v_depend_procdate1)

v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`

#判断前置依赖
v_cnt1=$(is_depend_success_with_no_prov $v_depend_procdate1 dwa_d_ia_basic_user_app)
echo "v_cnt1="$v_cnt1
if [ $v_cnt1 -eq 31 ]; then
#定义sql字符?*

v_sql="alter table dwa_d_cus_jingxun_internet_taxi drop partition (month_id='"$v_month"',day_id='"$v_day"');"
hive -e "
use $database;
$v_sql
;"


#定义sql字符?
v_sql="insert overwrite table dwa_d_cus_jingxun_internet_taxi partition(month_id = '"$v_month"', day_id = '"$v_day"')
select device_number, sum(visit_cnt) as visit_cnt
from ubd_dm.dwa_d_ia_basic_user_app
where month_id='"$v_depend_month1"' and day_id='"$v_depend_day1"' and prod_id in ('C281131', 'C237127') and length(device_number)=11
group by device_number
"
#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_d_cus_jingxun_internet_taxi_${v_month}_${v_day};
set mapreduce.job.queuename=ia_serv;
set mapred.job.priority=HIGH;
set hive.auto.convert.join=false;
set hive.exec.compress.output=true;
set hive.exec.reducers.bytes.per.reducer=2147483648;
set hive.groupby.skewindata=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.map.aggr=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=1073741824;
set hive.optimize.cp=true;
set hive.optimize.pruner=true;
set mapred.max.split.size=1073741824;
set mapred.min.split.size.per.node=536870912;
set mapred.min.split.size.per.rack=536870912;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set mapred.output.compression.type=BLOCK;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;

$v_sql;
" 2>&1 |tee $v_logfile >>/dev/null

#删除7天前的数据
v_sql="alter table dwa_d_cus_jingxun_internet_taxi drop partition (month_id='"$v_30month"',day_id='"$v_30day"');"
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
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_retinfo $v_retcode $v_rowline)
#****************************************
