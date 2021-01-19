#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_v_d_cus_vop_user_check.sh
# *功能描述     --%@COMMENT:      	三要素虚商数据加工
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-06-14
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:
# *修改记录     --%@MODIFY:       	2019-06-14 |  |
# *来源表       --%@FROM:           ubd_b_dwa.DWA_V_D_VOP_USER_INFO
# *目标表       --%@TO:             ubd_risk_serv.dm_d_cus_vop_user_check
###############################################################################
# 调用方法: bash p_dm_v_d_cus_vop_user_check.sh 201904614
###############################################################################

set -x
##函数引用
. ./p_pub_func_all_vop.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"
##***************************************************************************
#参数说明：该shell模板调用时需传入3个参数：$1为账期（yyyymm?$2为省分（xxx?$3为需求编码（xxx?
#例如：调用方法：./p_dwa_s_m_acc_al_charge.sh 201310 079 JZ_01_D_001
##***************************************************************************


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
v_part=`echo $((${v_month}%2))`
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
v_procname=p_dm_d_cus_vop_user_check
v_tablename=dm_d_cus_vop_user_check
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
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_vop_user_info)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate1=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
#使用的是 new_date 是部分省的 还有省份
v_depend_procdate1=$(new_date_vop $v_prov_099 dwa_v_d_vop_user_info)
echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename $v_depend_proc1 $v_depend_procdate1)
v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`
#判断前置依赖

v_cnt1=$(is_depend_success_with_no_prov $v_depend_procdate1 dwa_v_d_vop_user_info)
echo "v_cnt1="$v_cnt1
if [ $v_cnt1 -eq 1 ]; then
#定义sql字符?*


v_sql="alter table dm_d_cus_vop_user_check drop partition (month_id='"$v_month"',day_id='"$v_day"');"
hive -e "
use $database;
$v_sql
;"
v_sql="insert overwrite table dm_d_cus_vop_user_check partition(month_id = '"$v_month"', day_id = '"$v_day"')
select
    null area_no,
    null service_type,
    SVC_NUMBER device_number,
    upper(md5(SVC_NUMBER)) device_number_md5,
    sha256(SVC_NUMBER) device_number_sha256,
    case
        when innet_days < 20 then '1' 
        else '0' 
    end is_lit20,
    null is_rnet,
    null is_cert_numb5,
    upper(md5s(cust_name,'GBK')) cert_name_md5,
    upper(md5(cert_no)) cert_no_md5,
    sha256(cust_name) cert_name_sha256,
    sha256(cert_no) cert_no_sha256,
    cert_type,
    upper(md5(cert_type)) cert_type_md5,
    sha256(cert_type) cert_type_sha256,
    null is_card,
    MVNO_USER_STATUS is_innet,
    case 
        when cancel_time is null then null
        when cancel_time is not null and substr(cancel_time,1,8)>concat(month_id,day_id) then null
        else substr(cancel_time,1,8) 
    end close_date,
    null is_filter,
    case when MVNO_USER_STATUS='0' then '2'
    else '0' end status_id_mw,
    case 
        when cert_type='01' then '00'
        when cert_type='09' then '01'
        when cert_type='05' then '02'
        when cert_type='08' then '03'
        when cert_type='10' then '04'
        when cert_type='11' then '05'
        when cert_type='02' then '06'
        when cert_type='06' then '09'
        when cert_type='15' then '11'
        when cert_type='18' then '13'
        when cert_type='19' then '14'
        else '99'
    end cert_type_mw,
    case
        when nvl(innet_days, 0) <= 0 then '00'
        when innet_days >= 1 and innet_days <= 25 then '10'
        when innet_days >= 26 and innet_days <= 90 then '20'
        when innet_days >= 91 and innet_days <= 180 then '30'
        when innet_days >= 181 and innet_days <= 365 then '40'
        when innet_days >= 366 and innet_days <= 1095 then '50'
        when innet_days >= 1096 then '60'
    end innet_month_lvl_mw,
    null cert_numbs_mw,
    case 
        when MVNO_USER_STATUS='0' then floor(months_between(
            from_unixtime(unix_timestamp(substr(cancel_time,1,8),'yyyymmdd'),'yyyy-mm-dd'),
            from_unixtime(unix_timestamp(substr(deal_time,1,8),'yyyymmdd'),'yyyy-mm-dd')))
        when MVNO_USER_STATUS='1' then floor(months_between(
            from_unixtime(unix_timestamp(cast(concat(month_id,day_id) as string),'yyyymmdd'),'yyyy-mm-dd'),
            from_unixtime(unix_timestamp(substr(deal_time,1,8),'yyyymmdd'),'yyyy-mm-dd')))
    end innet_months,
    null stop_type,
    substr(deal_time,1,8) innet_date_mw,
    '1' is_valid_flag,
    null user_id,
    null user_id_md5,
    null user_id_sha256,
    null cert_numbs,
    null cert_innet_usernums,
    null cert_break_usernums,
    null cust_sex,
    null cert_age,
    null pay_mode,
    null zb_channel_no,
    concat(month_id,day_id) date_id,
    concat('0',prov_code) prov_id,
    $v_part,
    $v_month,
    $v_day,
    null cust_id,
    null constellation_desc
from
(
    select
        a.svc_number,a.deal_time,a.cust_name,a.cert_type,a.cert_no,a.prov_code,a.cancel_time,
        a.upd_time,a.mvno_user_status,a.user_type,a.month_id,a.day_id,a.innet_days
    from (
        select 
            svc_number,deal_time,cust_name,cert_type,cert_no,prov_code,cancel_time,
            upd_time,mvno_user_status,user_type,month_id,day_id,
            datediff(from_unixtime(unix_timestamp(cast(concat(month_id,day_id) as string),'yyyymmdd'),'yyyy-mm-dd'),
                from_unixtime(unix_timestamp(substr(deal_time,1,8),'yyyymmdd'),'yyyy-mm-dd'))+1 innet_days,
            row_number() over(partition by svc_number order by mvno_user_status desc, deal_time desc) sn
        from ubd_b_dwa.dwa_v_d_vop_user_info where month_id='"$v_depend_month1"' and day_id='"$v_depend_day1"'
    ) a where a.sn=1
)t;"

#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_d_cus_vop_user_check_$v_month_$v_day;
add jar ../jar/hive_encrypt_udf.jar;
create temporary function sha256 as 'com.unicom.hive.udf.SHA256';
add jar ../jar/md5s.jar;
CREATE TEMPORARY FUNCTION md5s AS 'wangchun.Md5s';
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

#删除7天前的数据
v_sql="alter table dm_d_cus_vop_user_check drop partition (month_id='"$v_7month"',day_id='"$v_7day"');"
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
