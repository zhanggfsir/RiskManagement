#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_v_d_cus_device_reuse_risk.sh
# *功能描述     --%@COMMENT:      	判断电话号码回收情况
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        张广峰
# *创建时间     --%@CREATED_TIME:   2019-04-17
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *修改记录     --%@MODIFY:       	2019-04-17 | 张广峰 | 
# *来源表       --%@FROM:           zba_dwd.dwd_m_prd_cb_user_list
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_cb_user_info
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_mb_user_info
# *目标表       --%@TO:             ubd_risk_serv.dm_v_d_cus_device_reuse_risk
###############################################################################
# 调用方法: bash p_dm_v_d_cus_device_reuse_risk.sh 20190417
###############################################################################

#set -x
##函数引用
. ./p_pub_func_all.sh

export HADOOP_CLIENT_OPTS="-Xmx2G"
##***************************************************************************
#参数说明：该shell模板调用时需传入3个参数：$1为账期（yyyymm?$2为省分
#例如：调用方法：./p_dwa_s_m_acc_al_charge.sh 201310 079 
##***************************************************************************


##********************************************
#声明变量,变量赋便
#需修改v_owner,v_pkg,v_provname,v_tab变量的便,过程参数顺序：${dateId} ${keywordMonth} ${prodMonth} ${provinceId}
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
v_last_day9=`date +%Y%m%d -d "$v_date -9 days"`
v_9month=`echo $v_last_day9 | cut -c 1-6`
v_9day=`echo $v_last_day9 | cut -c 7-8`
v_month=`echo $v_date | cut -c 1-6`
v_day=`echo $v_date | cut -c 7-8`
v_part=`echo $((${v_month}%2))`
v_part1=`echo $((${v_9month}%2))`
v_month1=$v_month'01'
v_month_bef1=`date -d "$v_month1 last month" "+%Y%m"`

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
v_procname=p_dm_v_d_cus_device_reuse_risk
v_tablename=dm_v_d_cus_device_reuse_risk
#获取mysql数据库连接（利用p_pub_func_log.sh中的check_mysql方法/函数进行mysql连接判断）
v_config_logmysql=$(check_mysql)
hostname=`echo $v_config_logmysql|awk -F: '{print $1}'`
port=`echo $v_config_logmysql|awk -F: '{print $2}'`
username=`echo $v_config_logmysql|awk -F: '{print $3}'`
password=`echo $v_config_logmysql|awk -F: '{print $4}'`
dbname=`echo $v_config_logmysql|awk -F: '{print $5}'`

#插入日志
$(insert_log $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的is_depend_success方法/函数进行获取】
isDependsuccess=$(is_depend_success $v_date $v_prov dwd_m_prd_cb_user_list)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate1=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的new_date方法/函数进行获取】
v_depend_procdate1=$(new_date $v_prov dwd_m_prd_cb_user_list)
echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc1 $v_depend_procdate1)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的is_depend_success方法/函数进行获取】
isDependsuccess=$(is_depend_success $v_date $v_prov dwa_v_d_cus_mb_user_info)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate2=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的new_date方法/函数进行获取】
v_depend_procdate2=$(new_date $v_prov dwa_v_d_cus_mb_user_info)
echo "v_depend_procdate2="$v_depend_procdate2
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc2 $v_depend_procdate2)
#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的is_depend_success方法/函数进行获取】
isDependsuccess=$(is_depend_success $v_date $v_prov dwa_v_d_cus_cb_user_info)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate4=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的new_date方法/函数进行获取】
v_depend_procdate4=$(new_date $v_prov dwa_v_d_cus_cb_user_info)
echo "v_depend_procdate4="$v_depend_procdate4
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc4 $v_depend_procdate4)

v_depend_month1=`echo $v_depend_procdate2 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate2 | cut -c 7-8`
v_depend_part1=`echo $((${v_depend_month1}%2))`
v_depend_month3=`echo $v_depend_procdate4 | cut -c 1-6`
v_depend_day3=`echo $v_depend_procdate4 | cut -c 7-8`
v_depend_part3=`echo $((${v_depend_month3}%2))`

#判断前置依赖

v_cnt1=$(data_depend_table $v_depend_procdate1 $v_prov dwd_m_prd_cb_user_list)
v_cnt2=$(data_depend_table $v_depend_procdate2 $v_prov dwa_v_d_cus_mb_user_info)
v_cnt4=$(data_depend_table $v_depend_procdate4 $v_prov dwa_v_d_cus_cb_user_info)
echo "v_cnt1="$v_cnt1
echo "v_cnt2="$v_cnt2
echo "v_cnt4="$v_cnt4
if [ $v_cnt1 -eq 1 -a $v_cnt2 -eq 1 -a $v_cnt4 -eq 1 ]; then
#定义sql字符?*

v_sql="alter table dm_v_d_cus_device_reuse_risk drop partition (month_id='"$v_month"',day_id='"$v_day"',prov_id='"$v_prov"');"
hive -e "
use ubd_risk_serv;
$v_sql
;" 

#/**
#4G有效非上网卡
#union all
#只是23G，没有转4G
#*/

#定义sql字符?
v_sql="insert into dm_v_d_cus_device_reuse_risk partition(month_id = '"$v_month"', day_id = '"$v_day"', prov_id = '"$v_prov"')
 select device_number,
         case
           when cnt >= 2 then
            0
           else
            1
         end reuse_type,
         '"${v_part}"','$v_prov','$v_month','$v_day'
    from (select device_number,  count(*) cnt
            from (
			select month_id,
                         prov_id,
                         day_id,
                         user_id,
                         device_number
                    from zba_dwa.dwa_v_d_cus_cb_user_info t
                   where month_id = '"$v_depend_month3"'
                     and day_id = '"$v_depend_day3"'
                     and prov_id = '"$v_prov"'
                     and service_type = '40AAAAAA'
                     and is_stat = '1'
                     and is_card = '0'
					 group by month_id,prov_id,day_id,user_id,device_number
					 
                  union all
				  
                  select t1.month_id, t1.prov_id, t.day_id, t.user_id, t.device_number
                    from (select user_id, device_number,  day_id
                            from zba_dwa.dwa_v_d_cus_mb_user_info t
                           where part_id = '"$v_depend_month1"'
                             and day_id = '"$v_depend_day1"'
                             and prov_id = '"$v_prov"'
                             and is_stat = '1') t
                    left join (select month_id,
                                     prov_id,
                                     user_id_prov,
                                     user_id_cbss
                                from zba_dwd.dwd_m_prd_cb_user_list
                               where month_id = '"$v_depend_month1"'
                                 and substr(cycle_id, 1, 6) <= '"$v_depend_month1"') t1
                      on t.user_id = t1.user_id_prov
                     where t1.user_id_prov is null
					 group by t1.month_id, t1.prov_id, t.day_id, t.user_id, t.device_number
					 ) t
           group by device_number) t
"
#hive执行sql命令，并将执行结果写入日志文件中
#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use ubd_risk_serv;
set mapred.job.name=risk_control@p_dm_v_d_cus_device_reuse_risk_$v_date_$v_prov;
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

v_sql="alter table dm_v_d_cus_device_reuse_risk drop partition (month_id='"$v_9month"',day_id='"$v_9day"',prov_id='"$v_prov"');"
hive -e "
use ubd_risk_serv;
#$v_sql
;"  

#获取过程执行情况（通过p_pub_func_analyze.sh中的is_exe_success方法/函数进行判断）
v_result=$(is_exe_success $v_logfile) >>/dev/null
if  [ $v_result -eq 1 ]; then
v_retcode=SUCCESS
v_retinfo=结束
#获取结果记录行数（通过p_pub_func_analyze.sh中的get_row_line_spare方法/函数进行判断）
v_rowline=$(get_row_line_spare $v_logfile) >>/dev/null
else
v_retcode=FAIL
#获取执行错误原因（通过p_pub_func_analyze.sh中的get_failed_info方法/函数进行判断）
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

