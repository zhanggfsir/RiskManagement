#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_m_cus_al_stop_info.sh
# *功能描述     --%@COMMENT:      	全业务用户近6月停机情况
# *执行周期     --%@PERIOD:         M
# *参数         --%@PARAM:          帐期 YYYY
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-06-05
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *修改记录     --%@MODIFY:       	2019-06-05
# *来源表       --%@FROM:           ubd_risk_serv.dm_m_cus_al_stop_info_mid
# *目标表       --%@TO:             ubd_risk_serv.dm_m_cus_al_stop_info
###############################################################################
# 调用方法: bash p_dm_m_cus_al_stop_info.sh yyyyMM 
#************************************************************** %*/

set -x
##函数引用
. ./p_pub_func_all.sh
export HADOOP_CLIENT_OPTS="-Xmx2G"

##***************************************************************************
#参数说明：该shell模板调用时需传入2个参数：$1为账期（yyyymm）$2为省分（xxx）
#例如：调用方法：./p_test.sh 201704 010
##***************************************************************************
##加工脚本输入参数
v_date=$1
v_prov=$2
v_prov_099=099
v_month=`echo $v_date | cut -c 1-6`
v_shellname=`basename $0` >>/dev/null
v_month1=$v_month'01'
v_3month=`date +%Y%m -d "$v_month1 -3 months"`
v_l6month=`date +%Y%m -d "-7 month $v_month1"`
v_last_month1=`date +%Y%m -d "$v_month1 -1 month"`
v_last_month2=`date +%Y%m -d "$v_month1 -2 month"`
v_last_month3=`date +%Y%m -d "$v_month1 -3 month"`
v_last_month4=`date +%Y%m -d "$v_month1 -4 month"`
v_last_month5=`date +%Y%m -d "$v_month1 -5 month"`

v_env=$3
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
echo "v_shellname1="$v_shellname
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
echo "v_shellname2="$v_shellname
 
##***************************************************************
##日志文件定义，确定日志文件存放的位置及日志文件名祿
##命名方式为：shell名称_账期_省分_系统时间憿log
##例如：p_dwa_s_m_acc_al_charge_201310_079_20131127172425.log
##规范：过程名统一小写
##***************************************************************
v_logfile=$(log_file $v_shellname $1 $2)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dm_m_cus_al_stop_info
v_tablename=dm_m_cus_al_stop_info
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
isDependsuccess=$(is_depend_success_risk  $v_date         $v_prov_099 dm_m_cus_al_stop_info_mid)
isDependsuccess1=$(is_depend_success_risk $v_last_month1  $v_prov_099 dm_m_cus_al_stop_info_mid)
isDependsuccess2=$(is_depend_success_risk $v_last_month2  $v_prov_099 dm_m_cus_al_stop_info_mid)
isDependsuccess3=$(is_depend_success_risk $v_last_month3  $v_prov_099 dm_m_cus_al_stop_info_mid)
isDependsuccess4=$(is_depend_success_risk $v_last_month4  $v_prov_099 dm_m_cus_al_stop_info_mid)
isDependsuccess5=$(is_depend_success_risk $v_last_month5  $v_prov_099 dm_m_cus_al_stop_info_mid)


if [ $isDependsuccess -eq 1 -a $isDependsuccess1 -eq 1 -a $isDependsuccess2 -eq 1 -a $isDependsuccess3 -eq 1 -a $isDependsuccess4 -eq 1 -a $isDependsuccess5 -eq 1  ]; then
#定义sql字符?
v_sql="alter table dm_m_cus_al_stop_info drop partition (month_id='"$v_month"',prov_id='"$v_prov"');"
hive -e "
use $database;
$v_sql
;" 

#定义sql字符?
v_sql="insert into dm_m_cus_al_stop_info partition (month_id='"$v_month"',prov_id='"$v_prov"')
   select upper(md5(device_number))device_number_md5,
  sum(case when substr(last_stop_date, 1, 6) = '"$v_month"' then 1 else 0 end),
  sum(case when month_id >= '"$v_month"'       then 1 else 0 end),
  sum(case when month_id >= '"$v_last_month1"' then 1 else 0 end),
  sum(case when month_id >= '"$v_last_month2"' then 1 else 0 end),
  sum(case when month_id >= '"$v_last_month3"' then 1 else 0 end),
  sum(case when month_id >= '"$v_last_month4"' then 1 else 0 end),
  sum(case when month_id >= '"$v_last_month5"' then 1 else 0 end)
    from $database.dm_m_cus_al_stop_info_mid
   where month_id between '"$v_last_month5"' and '"$v_month"'
     and prov_id = '"$v_prov"'
   group by device_number
  
  "

#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_m_cus_al_stop_info_${v_month}_${v_prov};
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
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
$v_sql
;" 2>&1 |tee $v_logfile >>/dev/null 

v_sql="alter table dm_m_cus_al_stop_info drop partition 
(month_id='"$v_3month"',prov_id='"$v_prov"');"
hive -e "
use $database;
$v_sql
;" 

#获取过程执行情况（通过p_pub_func_analyze.sh中的isExeSuccess方法/函数进行判断）
v_result=$(is_exe_success $v_logfile) >>/dev/null
#echo "v_result==============="$v_result
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
