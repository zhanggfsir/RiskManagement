#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_m_use_all_cz_info.sh
# *功能描述     --%@COMMENT:      	用户目标月通话最多的城市
# *执行周期     --%@PERIOD:         M
# *参数         --%@PARAM:          帐期 YYYYMM
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-06-18
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *来源表       --%@FROM:           ubd_risk_serv.dm_v_d_use_mb_voice_payeco_risk
# *来源表       --%@FROM:           ubd_risk_serv.dm_v_d_use_cb_voice_payeco_risk
# *目标表       --%@TO:             ubd_risk_serv.dm_m_use_all_cz_info
###############################################################################
# 调用方法: bash p_dm_m_use_all_cz_info.sh 201904 011
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
v_last_date=$(get_last_day $v_month)
v_month01="$v_date"01
v_3month=`date +%Y%m -d "$v_month01 -3 months"`
v_shellname=`basename $0` >>/dev/null
echo "v_shellname1="$v_shellname
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
echo "v_shellname2="$v_shellname
 
v_logfile=$(log_file $v_shellname $1 $v_prov)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dm_m_use_all_cz_info
v_tablename=dm_m_use_all_cz_info

v_depend_proc1=dm_v_d_use_mb_voice_payeco_risk
v_depend_proc2=dm_v_d_use_cb_voice_payeco_risk


#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`


#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)


#v_depend_procdate1
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess1=$(is_depend_success_risk $v_last_date $v_prov dm_v_d_use_mb_voice_payeco_risk)
if [ $isDependsuccess1 -eq 1 ]; then
v_depend_procdate1=$v_last_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate1=$(new_date_risk $v_prov dm_v_d_use_mb_voice_payeco_risk)
echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc1 $v_depend_procdate1)

#v_depend_procdate2
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_risk $v_last_date $v_prov dm_v_d_use_cb_voice_payeco_risk)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate2=$v_last_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate2=$(new_date_risk $v_prov dm_v_d_use_cb_voice_payeco_risk)
echo "v_depend_procdate2="$v_depend_procdate2
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc2 $v_depend_procdate2)

v_cnt1=$(data_depend_table_risk $v_depend_procdate1 $v_prov dm_v_d_use_mb_voice_payeco_risk)
v_cnt2=$(data_depend_table_risk $v_depend_procdate2 $v_prov dm_v_d_use_cb_voice_payeco_risk)

v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`
v_depend_month2=`echo $v_depend_procdate2 | cut -c 1-6`
v_depend_day2=`echo $v_depend_procdate2 | cut -c 7-8`
#========================================================================
#if [ 31 -eq 31 ]; then
#if [ $v_cnt1 -eq 1 -a $v_cnt2 -eq 1 ]; then
if [ $v_cnt2 -eq 1 ]; then

#定义sql字符?*
v_sql="alter table dm_m_use_all_cz_info drop partition (month_id='"$v_month"',prov_id='"$v_prov"');"
hive -e "
use $database;
$v_sql
;" 


#定义sql字符?

v_sql1="insert into table dm_m_use_all_cz_info partition(month_id='"$v_month"',prov_id='"$v_prov"',sa_type='MB')
select t1.device_number as device_number_md5,
       t1.prov_id_lacci,
       t1.area_id_lacci,
       t1.address_days,
        ROW_NUMBER() OVER(PARTITION BY t1.DEVICE_NUMBER ORDER BY t1.address_days DESC) m
  from (select device_number,
               prov_id_lacci,
               area_id_lacci,
               count(*) address_days
          from ubd_risk_serv.dm_v_d_use_mb_voice_payeco_risk t
         where part_id = '$v_part'
           and prov_id = '$v_prov'
           and mcc in ('460','461', '999')
         group by device_number,
                  prov_id,
                  prov_id_lacci,
                  area_id_lacci) t1
"

v_sql2="insert into table dm_m_use_all_cz_info partition(month_id='"$v_month"',prov_id='"$v_prov"',sa_type='CB')
select t1.device_number as device_number_md5,
       t1.prov_id_lacci,
       t1.area_id_lacci,
       t1.address_days,       
        ROW_NUMBER() OVER(PARTITION BY t1.DEVICE_NUMBER ORDER BY t1.address_days DESC) m
  from (select device_number,
               prov_id_lacci,
               area_id_lacci,
               count(*) address_days
          from ubd_risk_serv.dm_v_d_use_cb_voice_payeco_risk t
         where part_id = '$v_part'
           and prov_id = '$v_prov'
           and mcc in ('460','461', '999')
         group by device_number,
                  prov_id,
                  prov_id_lacci,
                  area_id_lacci) t1"

hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_m_use_all_cz_info_${v_month}_${v_prov};
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
$v_sql1;
$v_sql2;
" 2>&1 |tee $v_logfile >>/dev/null 

#删除3月前的数据 
v_sql="alter table dm_m_use_all_cz_info drop partition (month_id='"$v_3month"',prov_id='"$v_prov"');"
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
#****************************************


if [ $v_prov -eq 088 -o $v_prov -eq 079 ] ; then
#更新日志
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_retinfo $v_retcode 1)
#****************************************
fi

