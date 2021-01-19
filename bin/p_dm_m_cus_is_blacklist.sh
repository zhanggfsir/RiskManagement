#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_m_cus_is_blacklist.sh
# *功能描述     --%@COMMENT:      	欠费黑名单
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-06-05
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:
# *修改记录     --%@MODIFY:       	2019-06-05
# *来源表       --%@FROM:           ubd_dm.dwa_v_m_cus_al_user_info
# *来源表       --%@FROM:           ubd_dm.dwa_v_m_cus_al_owe_e
# *来源表       --%@FROM:           ubd_dm.dwa_s_m_acc_cb_owe
# *来源表       --%@FROM:           ubd_dm.dwd_d_cus_al_redblacklist
# *来源表       --%@FROM:           ubd_dm.dwd_d_cus_cb_cust_speclist
# *目标表       --%@TO:             ubd_risk_serv.dm_m_cus_is_blacklist
###############################################################################
# 调用方法: bash p_dm_m_cus_is_blacklist.sh yyyyMM
###############################################################################
set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"

##***************************************************************************
#参数说明：该shell模板调用时需传入2个参数：$1为账期（yyyymm）$2为省份（xxx）
#例如：调用方法：./p_dm_v_m_cus_is_blacklist.sh 201906 011
##***************************************************************************
##加工脚本输入参数
v_date=$1
v_prov=$2
v_prov_099=099
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

v_month=`echo $v_date | cut -c 1-6`
v_part=`echo $((${v_month}%2))`
v_last_day7=`date +%Y%m%d -d "$v_date -7 days"`
v_month01="$v_date"01
v_3month=`date +%Y%m -d "$v_month01 -3 months"`
v_7month=`echo $v_last_day7 | cut -c 1-6`
v_7day=`echo $v_last_day7 | cut -c 7-8`
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
v_logfile=$(log_file $v_shellname $1 $v_prov)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dm_m_cus_is_blacklist
v_tablename=dm_m_cus_is_blacklist
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
isDependsuccess2=$(is_depend_success $v_date $v_prov dwa_s_m_acc_cb_owe)
isDependsuccess3=$(is_depend_success $v_date $v_prov dwa_s_m_cus_al_owe_e)

#v_depend_procdate4
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success $v_last_date $v_prov dwd_d_cus_al_redblacklist)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate4=$v_last_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate4=$(new_date $v_prov dwd_d_cus_al_redblacklist)
echo "v_depend_procdate4="$v_depend_procdate4
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc2 $v_depend_procdate4)

#v_depend_procdate5
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success $v_last_date $v_prov dwd_d_cus_cb_cust_speclist)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate5=$v_last_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate5=$(new_date $v_prov dwd_d_cus_cb_cust_speclist)
echo "v_depend_procdate5="$v_depend_procdate5
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc2 $v_depend_procdate5)

v_cnt4=$(data_depend_table $v_depend_procdate4 $v_prov dwd_d_cus_al_redblacklist)
v_cnt5=$(data_depend_table $v_depend_procdate5 $v_prov dwd_d_cus_cb_cust_speclist)

echo "v_depend_procdate4="	$v_depend_procdate4
v_depend4_month=`echo $v_depend_procdate4 | cut -c 1-6`
v_depend4_day=`echo $v_depend_procdate4 | cut -c 7-8`
echo "v_depend_procdate5="	$v_depend_procdate5
v_depend5_month=`echo $v_depend_procdate5 | cut -c 1-6`
v_depend5_day=`echo $v_depend_procdate5 | cut -c 7-8`

echo "v_cnt4="	$v_cnt4
echo "v_cnt5="	$v_cnt5

#判断前置依赖


#if [ 1 -eq 1 ]; then
if [ $isDependsuccess1 -eq 1 -a $isDependsuccess2 -eq 1 -a $isDependsuccess3 -eq 1  -a $v_cnt4 -eq 1 -a $v_cnt5 -eq 1 ]; then
#定义sql字符?*

v_sql="alter table dm_m_cus_is_blacklist drop partition (month_id='"$v_month"',prov_id='"$v_prov"');"
hive -e "
use $database;
$v_sql
;"

#定义sql字符?
v_sql="insert overwrite table dm_m_cus_is_blacklist partition(month_id = '"$v_month"',prov_id='"$v_prov"')
select
t1.device_number_md5,
case
when t4.user_id is not null and nvl(t3.owe_fee, 0) > 0 then
'1'
when t5.user_id is not null and nvl(t2.owe_fee, 0) > 0 then
'1'
else
'0' end is_blacklist
from
(select user_id,upper(md5(device_number)) device_number_md5
from ubd_dm.dwa_v_m_cus_al_user_info
where month_id = '$v_date'
and prov_id = '$v_prov'
and is_stat = '1')t1

left join
(select user_id,
all_owe_fee as owe_fee
from (select
user_id,
sum(owe_fee) over(partition by user_id) all_owe_fee
from (select user_id,
sum(owe_fee) owe_fee
from ubd_dm.dwa_s_m_acc_cb_owe
where month_id = '$v_date'
and prov_id = '$v_prov'
and owe_fee > 0
and acct_month <= '$v_date'
group by user_id) tmp) t2
group by user_id,
all_owe_fee) t2
on t1.user_id = t2.user_id

left join (select subs_instance_id,
all_owe_fee as owe_fee
from (select subs_instance_id,
sum(owe_fee) over(partition by subs_instance_id) all_owe_fee
from (select user_id as subs_instance_id,
sum(owe_fee) owe_fee
from ubd_dm.dwa_s_m_cus_al_owe_e
where month_id = '$v_date'
and prov_id = '$v_prov'
and owe_fee > 0
and owe_month <= '$v_date'
group by user_id) tmp1) tmp2
group by subs_instance_id,
all_owe_fee) t3
on t1.user_id = t3.subs_instance_id

left join
(select user_id
from zba_dwd.dwd_d_cus_al_redblacklist t
where t.month_id = '$v_depend4_month'
and t.prov_id = '$v_prov'
and t.day_id = '$v_depend4_day'
and t.is_valid = '1'
and t.list_type = '02'
group by user_id) t4
on t1.user_id = t4.user_id
left join
(select user_id
from zba_dwd.dwd_d_cus_cb_cust_speclist t
where t.month_id = '$v_depend5_month'
and t.day_id = '$v_depend5_day'
and t.prov_id = '$v_prov'
and t.list_type = '01'
group by user_id) t5
on t1.user_id = t5.user_id"

#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_v_m_cus_is_blacklist_$v_month_$v_prov;
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
v_sql="alter table dm_m_cus_is_blacklist drop partition (month_id='"$v_3month"',prov_id='"$v_prov"');"
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