#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_v_d_use_mb_voice_payeco_risk.sh
# *功能描述     --%@COMMENT:      	MBSS易联支付位置信息(日) 
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        张广峰
# *创建时间     --%@CREATED_TIME:   2019-04-17
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:	
# *修改记录     --%@MODIFY:       	2019-04-17 | 张广峰 | 
# *来源表       --%@FROM:           ubd_dm.dwd_d_use_mb_voice
# *来源表       --%@FROM:           ubd_dm.dim_m_cell_zyzx_all
# *来源表       --%@FROM:           ubd_dm.dim_country_code_all
# *来源表       --%@FROM:           ubd_dm.dim_area_code
# *目标表       --%@TO:             ubd_risk_serv.dm_v_d_use_mb_voice_payeco_risk
###############################################################################
# 调用方法: bash p_dm_v_d_use_mb_voice_payeco_risk.sh 20190417
###############################################################################

#set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"
##***************************************************************************
#参数说明：该shell模板调用时需传入2个参数：$1为账期（yyyymm?$2为省分（xxx?
#例如：调用方法：./p_dwa_s_m_acc_al_charge.sh 201310 079
##***************************************************************************

v_date=$1
v_prov=$2
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
v_day=`echo $v_date | cut -c 7-8`
v_part=`echo $((${v_month}%2))`
v_month1=$v_month'01'
v_month_bef1=`date -d "$v_month1 last month" "+%Y%m"`

v_month1=$v_date'01'
last_month6=`date +%Y%m -d "-7 month $v_month1"`

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
v_logfile=$(log_file $v_shellname $1 $2)

##判断日志文件是否存在，如果存在就清空

if [ -f $v_logfile ]
 then
   cat /dev/null > $v_logfile
fi

#私有参数初始化（根据脚本自行进行调整及配置）
v_procname=p_dm_v_d_use_mb_voice_payeco_risk
v_tablename=dm_v_d_use_mb_voice_payeco_risk
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
isDependsuccess=$(is_depend_success $v_date $v_prov dwd_d_use_mb_voice)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate1=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate1=$(new_date $v_prov dwd_d_use_mb_voice)
echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc1 $v_depend_procdate1)

v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`
v_depend_part1=`echo $((${v_depend_month1}%2))`

#判断前置依赖  

v_cnt1=$(data_depend_table $v_depend_procdate1 $v_prov dwd_d_use_mb_voice)

echo "v_cnt1="$v_cnt1
#if [ 1 -eq 1 ]; then
if [ $v_cnt1 -eq 1 ]; then

#定义sql字符?
v_sql="alter table dm_v_d_use_mb_voice_payeco_risk drop partition(part_id='"$v_part"',day_id='"$v_day"',prov_id='"$v_prov"') "
hive -e "
use $database;
$v_sql
;" 

v_sql="insert overwrite table dm_v_d_use_mb_voice_payeco_risk partition(part_id='"$v_part"',day_id='"$v_day"',prov_id='"$v_prov"')
SELECT  '$v_month',
        UPPER(MD5(T.USER_ID)),
        UPPER(MD5(T.DEVICE_NUMBER)),
        T.SERVICE_TYPE,
        A.LAC,
        A.CELL_ID,
        coalesce(A.AREA_ID_NEW,D.AREA_ID_NEW,'999'),
        coalesce(A.PROV_ID_NEW,D.PROV_ID,'999'),
        coalesce(C.MCC,'999'),
		'$v_prov','$v_day'
  FROM (select 
         user_id,device_number,service_type,inter_oper_net_type,lac,cell_id,visit_area_code,
         case when (trim(T.lac) regexp '^[0-9]*$')='true' then cast(trim(T.LAC) as int) ELSE T.LAC END lac_new,
         case when (trim(T.CELL_ID) regexp '^[0-9]*$')='true' then cast(trim(T.CELL_ID) as int)  else T.CELL_ID end 
 CELL_ID_new
        from ubd_dm.DWD_D_USE_MB_VOICE t 
        where  day_id='"$v_depend_day1"' and prov_id='"$v_prov"'  and part_id='"$v_depend_month1"') T
          LEFT OUTER JOIN (SELECT prov_id as PROV_ID_NEW,area_id as AREA_ID_NEW,lac_tac as LAC,eci_ci as CELL_ID FROM ubd_dm.dim_m_cell_zyzx_all group by prov_id,area_id,lac_tac,eci_ci) A
    ON t.lac_new= A.LAC
   AND t.CELL_ID_new = A.CELL_ID
   LEFT OUTER JOIN (SELECT MCC
             FROM ubd_dm.dim_country_code_all 
             GROUP BY MCC) C
   ON SUBSTR(T.INTER_OPER_NET_TYPE,1,3) = C.MCC
   LEFT OUTER JOIN (select AREA_ID_NEW,AREA_CODE,PROV_ID
       from (select AREA_ID_NEW,AREA_CODE,PROV_ID,row_number() over(partition by AREA_CODE order by area_id_new asc) rn
               from  ubd_dm.dim_area_code where area_code is not null ) t
              where rn = 1) D
   ON case when substr(t.visit_area_code,1,1)<>'0' then concat('0',t.visit_area_code) 
           when t.visit_area_code='U029' OR t.visit_area_code='0290' then '029' else t.visit_area_code end= D.AREA_CODE
   group by     UPPER(MD5(T.USER_ID)),
                UPPER(MD5(T.DEVICE_NUMBER)),
                T.SERVICE_TYPE,
                A.LAC,
                A.CELL_ID,
                coalesce(A.AREA_ID_NEW,D.AREA_ID_NEW,'999'),
                coalesce(A.PROV_ID_NEW,D.PROV_ID,'999'),
                coalesce(C.MCC,'999');"

#hive执行sql命令，并将执行结果写入日志文件中
hive -e " 
use $database;
set mapred.job.name=risk_control@p_dm_v_d_use_mb_voice_payeco_risk-${v_date}_${v_prov};
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

#获取过程执行情况（通过p_pub_func_analyze.sh中的is_exe_success方法/函数进行判断）
v_result=$(is_exe_success $v_logfile) >>/dev/null
#echo "v_result==============="$v_result
if  [ $v_result -eq 1 ]; then
v_retcode=SUCCESS
v_retinfo=结束
#获取结果记录行数（通过p_pub_func_analyze.sh中的get_row_line_spare方法/函数进行判断）
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


