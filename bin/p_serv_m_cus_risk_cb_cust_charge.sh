#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:          serv_m_cus_risk_cb_cust_charge.sh
# *功能描述     --%@COMMENT:        客户消费表
# *执行周期     --%@PERIOD:         M
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *参数         --%@PARAM:          省分
# *创建人       --%@CREATOR:        王玮
# *创建时间     --%@CREATED_TIME:   2019-08-13
# *层次         --%@LEVEL:          DWA
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:         
# *修改记录     --%@MODIFY:         
# *来源表       --%@FROM:          ubd_dm.dwa_v_m_cus_nm_charge
# *来源表       --%@FROM:          ubd_dm.dwa_v_d_cus_al_user_info
# *目标表       --%@TO:            ubd_risk_serv.serv_m_cus_risk_cb_cust_charge
###############################################################################
# 调用方法: sh serv_m_cus_risk_cb_cust_charge.sh 20190302 release
###############################################################################
set -x;
# 函数引用，使用相对路径
. ./p_pub_func_all.sh
###############################################################################
# 环境变量设置
export HADOOP_CLIENT_OPTS="-Xmx2G"
###############################################################################
##加工脚本输入参数
v_date=$1
v_month_end=$(get_last_day $v_date)
v_month=`echo $v_month_end | cut -c 1-6`  
v_day=`echo $v_month_end | cut -c 7-8`
v_month1=$v_month'01'
v_12month=`date +%Y%m -d "$v_month1 -12 month"`
v_prov=099
v_rowline=0
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
###############################################################################
v_shellname=`basename $0` >>/dev/null
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
v_logfile=$(log_file $v_shellname $1 099)
# 判断日志文件是否存在，如果存在就清空
if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi
###############################################################################
#获取mysql数据库连【利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断】
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

v_procname=P_SERV_M_CUS_RISK_CB_CUST_CHARGE
v_tablename=SERV_M_CUS_RISK_CB_CUST_CHARGE
v_depend_proc1=DWA_V_M_CUS_NM_CHARGE
v_depend_proc2=DWA_V_D_CUS_AL_USER_INFO
#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)
###############################################################################
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess1=$(is_depend_success_with_no_prov $v_month $v_depend_proc1)  

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess2=$(is_depend_success_with_no_prov $v_month_end $v_depend_proc2)

if [  $isDependsuccess1 -eq 31 -a $isDependsuccess2 -eq 1 ]; then
###############################################################################
v_sql="insert overwrite table  serv_m_cus_risk_cb_cust_charge  partition(month_id='"$v_month"',prov_id)
select 
    CUST_ID,
    sum(total_fee) as total_fee,
    prov_id 
from(   
    select
        a.CUST_ID,
        a.total_fee,
        a.prov_id
    from (
        select CUST_ID,total_fee,PROV_ID 
        from ubd_dm.dwa_v_m_cus_nm_charge 
        where month_id='"$v_month"'  and is_acct=1
    ) a
    inner join (
        select 
        CUST_ID,prov_id
        from ubd_dm.dwa_v_d_cus_al_user_info 
        where (is_innet=1 or is_this_break=1) and is_stat=1 and substr(service_type,1,2)='40' and 
        part_id='"$v_month"' and day_id='"$v_day"' 
        group by CUST_ID,prov_id
    )b on a.CUST_ID=b.CUST_ID and a.PROV_ID=b.PROV_ID)t
group by CUST_ID,prov_id;"
#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_serv_m_cus_risk_cb_cust_charge_${v_month};
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
$v_sql
;" 2>&1 |tee $v_logfile >>/dev/null 

v_sql="alter table serv_m_cus_risk_cb_cust_charge drop partition(month_id='"$v_12month"');"
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
fi

else
v_retcode=WAIT
v_retinfo=等待
fi
echo $v_retcode

#更新日志
$(update_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_retinfo $v_retcode $v_rowline)
#****************************************
