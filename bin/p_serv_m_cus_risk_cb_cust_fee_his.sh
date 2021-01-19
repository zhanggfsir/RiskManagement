#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           serv_m_cus_risk_cb_cust_fee_his.sh
# *功能描述     --%@COMMENT:        客户消费历史表
# *执行周期     --%@PERIOD:         M
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *参数         --%@PARAM:          省分
# *创建人       --%@CREATOR:        王玮
# *创建时间     --%@CREATED_TIME:   2019-08-27
# *层次         --%@LEVEL:          DWA
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:         
# *修改记录     --%@MODIFY:         
# *来源表       --%@FROM:          ubd_risk_serv.serv_m_cus_risk_cb_cust_charge
# *来源表       --%@FROM:          ubd_dm.dwa_v_m_cus_cbm_fee_his
# *来源表       --%@FROM:          ubd_dm.dwa_v_d_cus_al_user_info
# *目标表       --%@TO:            ubd_risk_serv.serv_m_cus_risk_cb_cust_fee_his
###############################################################################
# 调用方法: sh serv_m_cus_risk_cb_cust_fee_his.sh 201911 release
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
v_last_month1=`date +%Y%m -d "$v_month1 -1 month"`
v_last_month2=`date +%Y%m -d "$v_month1 -2 month"`
v_last_month3=`date +%Y%m -d "$v_month1 -3 month"`
v_last_month4=`date +%Y%m -d "$v_month1 -4 month"`
v_last_month5=`date +%Y%m -d "$v_month1 -5 month"`
v_last_month6=`date +%Y%m -d "$v_month1 -6 month"`
v_last_month7=`date +%Y%m -d "$v_month1 -7 month"`
v_last_month8=`date +%Y%m -d "$v_month1 -8 month"`
v_last_month9=`date +%Y%m -d "$v_month1 -9 month"`
v_last_month10=`date +%Y%m -d "$v_month1 -10 month"`
v_last_month11=`date +%Y%m -d "$v_month1 -11 month"`
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
v_prov=099
v_rowline=0

###############################################################################
v_shellname=`basename $0` >>/dev/null
v_shellname=`echo $v_shellname|awk -F"." '{print $1}'` >>/dev/null
v_logfile=$(log_file $v_shellname $1 $v_prov)
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
#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=P_SERV_M_CUS_RISK_CB_CUST_FEE_HIS
v_tablename=SERV_M_CUS_RISK_CB_CUST_FEE_HIS
v_depend_proc1=SERV_M_CUS_RISK_CB_CUST_CHARGE  
v_depend_proc2=DWA_V_M_CUS_CBM_FEE_HIS   
v_depend_proc3=DWA_V_D_CUS_AL_USER_INFO
#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)
###############################################################################

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess1=$(is_depend_success_with_no_prov_risk $v_month $v_depend_proc1)  

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess2=$(is_depend_success_with_no_prov $v_month $v_depend_proc2)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess3=$(is_depend_success_with_no_prov $v_month_end $v_depend_proc3)
if [  $isDependsuccess1 -eq 1 -a $isDependsuccess2 -eq 31 -a $isDependsuccess3 -eq 1 ]; then

###############################################################################
v_sql="insert overwrite table  serv_m_cus_risk_cb_cust_fee_his partition(month_id='"$v_month"',prov_id)
    select 
    t1.CUST_ID,
    cast(nvl(t1.this_fee,t4.this_fee)      as decimal(20,2)) this_fee,
    cast(nvl(t1.last_fee,t4.last_1_fee)    as decimal(20,2)) last_fee,
    cast(nvl(t1.last2_fee,t4.last_2_fee)   as decimal(20,2)) last2_fee,
    cast(nvl(t1.last3_fee,t4.last_3_fee)   as decimal(20,2)) last3_fee,
    cast(nvl(t1.last4_fee,t4.last_4_fee)   as decimal(20,2)) last4_fee,
    cast(nvl(t1.last5_fee,t4.last_5_fee)   as decimal(20,2)) last5_fee,
    cast(nvl(t1.last6_fee,t4.last_6_fee)   as decimal(20,2)) last6_fee,
    cast(nvl(t1.last7_fee,t4.last_7_fee)   as decimal(20,2)) last7_fee,
    cast(nvl(t1.last8_fee,t4.last_8_fee)   as decimal(20,2)) last8_fee,
    cast(nvl(t1.last9_fee,t4.last_9_fee)   as decimal(20,2)) last9_fee,
    cast(nvl(t1.last10_fee,t4.last_10_fee) as decimal(20,2)) last10_fee,
    cast(nvl(t1.last11_fee,t4.last_11_fee) as decimal(20,2)) last11_fee,
    cast(nvl(t1.this_fee,t4.this_fee)+nvl(t1.last_fee,t4.last_1_fee)+
    nvl(t1.last2_fee,t4.last_2_fee)+nvl(t1.last3_fee,t4.last_3_fee)+
    nvl(t1.last4_fee,t4.last_4_fee)+nvl(t1.last5_fee,t4.last_5_fee)+
    nvl(t1.last6_fee,t4.last_6_fee)+nvl(t1.last7_fee,t4.last_7_fee)+
    nvl(t1.last8_fee,t4.last_8_fee)+nvl(t1.last9_fee,t4.last_9_fee)+
    nvl(t1.last10_fee,t4.last_10_fee)+nvl(t1.last11_fee,t4.last_11_fee)  as decimal(20,2)) total_year_fee,
    t1.prov_id
    from
    (
    select
    CUST_ID,
    sum(case when month_id='"$v_month"' then total_fee else null end) as  this_fee,
    sum(case when month_id='"$v_last_month1"' then total_fee else null end) as last_fee,
    sum(case when month_id='"$v_last_month2"' then total_fee else null end) as last2_fee,
    sum(case when month_id='"$v_last_month3"' then total_fee else null end) as last3_fee,
    sum(case when month_id='"$v_last_month4"' then total_fee else null end) as last4_fee,
    sum(case when month_id='"$v_last_month5"' then total_fee else null end) as last5_fee,
    sum(case when month_id='"$v_last_month6"' then total_fee else null end) as last6_fee,
    sum(case when month_id='"$v_last_month7"' then total_fee else null end) as last7_fee,
    sum(case when month_id='"$v_last_month8"' then total_fee else null end) as last8_fee,
    sum(case when month_id='"$v_last_month9"' then total_fee else null end) as last9_fee,
    sum(case when month_id='"$v_last_month10"' then total_fee else null end) as last10_fee,
    sum(case when month_id='"$v_last_month11"' then total_fee else null end) as last11_fee,
    prov_id
    from ubd_risk_serv.serv_m_cus_risk_cb_cust_charge
    where month_id between '"$v_last_month11"' and '"$v_month"'
    group by cust_id,prov_id
    ) t1
    left join
    (
    select
    t3.CUST_ID,                  
    t2.this_fee,         
    t2.last_1_fee,        
    t2.last_2_fee,        
    t2.last_3_fee,        
    t2.last_4_fee,        
    t2.last_5_fee,        
    t2.last_6_fee,        
    t2.last_7_fee,        
    t2.last_8_fee,        
    t2.last_9_fee,        
    t2.last_10_fee,       
    t2.last_11_fee,
    t2.prov_id
from
     (select
     user_id,
     this_fee,  
     last_1_fee,
     last_2_fee,
     last_3_fee,
     last_4_fee,
     last_5_fee,
     last_6_fee,
     last_7_fee,
     last_8_fee,
     last_9_fee,
     last_10_fee,
     last_11_fee,
     prov_id
from   
    ubd_dm.dwa_v_m_cus_cbm_fee_his 
    where month_id='"$v_month"')t2
    inner join
    (select PROV_ID,CUST_ID,USER_ID from ubd_dm.dwa_v_d_cus_al_user_info where part_id='"$v_month"' and day_id='"$v_day"' and CUST_ID is not null) t3
    on t2.user_id=t3.user_id and t2.prov_id=t3.prov_id
    )t4
    on t1.CUST_ID=t4.CUST_ID and t1.prov_id=t4.prov_id
    ;"
#########################################################################
#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_serv_m_cus_risk_cb_cust_fee_his_$v_month;
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

v_sql="alter table serv_m_cus_risk_cb_cust_fee_his drop partition(month_id='"$v_12month"');"
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

