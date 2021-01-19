#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hive
# *名称         --%@NAME:           p_dm_v_d_cus_user_check.sh
# *功能描述     --%@COMMENT:      	三要素数据加工
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        张广峰
# *创建时间     --%@CREATED_TIME:   2019-04-17
# *层次         --%@LEVEL:          UBD_SERV
# *数据域       --%@DOMAIN:         RISK域
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *修改记录     --%@MODIFY:       	2019-04-17 | 张广峰 | 
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_al_user_info
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_al_cust_info
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_cb_rns_encap
# *来源表       --%@FROM:           zba_dwa.dwa_v_d_cus_al_rns_encap
# *来源表       --%@FROM:           zba_dwa.dwa_v_m_cus_al_rns_wide
# *目标表       --%@TO:             ubd_risk_serv.dm_v_d_cus_user_check
###############################################################################
# 调用方法: bash p_dm_v_d_cus_user_check.sh 20190417
###############################################################################

set -x
##函数引用
. ./p_pub_func_all.sh

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
v_procname=p_dm_v_d_cus_user_check
v_tablename=dm_v_d_cus_user_check
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
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_al_user_info)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate1=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
#使用的是 new_date 是部分省的 还有省份
v_depend_procdate1=$(new_date $v_prov_099 dwa_v_d_cus_al_user_info)
echo "v_depend_procdate1="$v_depend_procdate1
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov_099 $v_tablename $v_depend_proc1 $v_depend_procdate1)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_al_cust_info)
if [ $isDependsuccess -eq 31 ]; then
v_depend_procdate2=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate2=$(new_date_with_no_prov dwa_v_d_cus_al_cust_info)
echo "v_depend_procdate2="$v_depend_procdate2
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc2 $v_depend_procdate2)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_cb_rns_encap)
if [ $isDependsuccess -eq 31 ]; then
v_depend_procdate3=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate3=$(new_date_with_no_prov dwa_v_d_cus_cb_rns_encap)
echo "v_depend_procdate3="$v_depend_procdate3
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc3 $v_depend_procdate3)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_with_no_prov $v_date dwa_v_d_cus_al_rns_encap)
if [ $isDependsuccess -eq 31 ]; then
v_depend_procdate4=$v_date
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate4=$(new_date_with_no_prov dwa_v_d_cus_al_rns_encap)
echo "v_depend_procdate3="$v_depend_procdate4
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc4 $v_depend_procdate4)

#判断前置是否正常工（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的isDependsuccess方法/函数进行获取】
isDependsuccess=$(is_depend_success_with_no_prov $v_month dwa_v_m_cus_al_rns_wide)
if [ $isDependsuccess -eq 1 ]; then
v_depend_procdate5=$v_month
else
#获取前置表的最大账期（每个前置信息表均需要配置）【利用p_pub_func_analyze.sh中的newDate方法/函数进行获取】
v_depend_procdate5=$(new_date_with_no_prov dwa_v_m_cus_al_rns_wide)
echo "v_depend_procdate3="$v_depend_procdate4
fi
#记录加工数据前置信息（每个前置信息表均需要配置）【利用p_pub_func_log.sh中的insertDependdate方法/函数进行加工】
$(insert_depend_date $hostname $port $username $password $dbname $v_date $v_pkg $v_procname $v_prov $v_tablename $v_depend_proc4 $v_depend_procdate5)


v_depend_month1=`echo $v_depend_procdate1 | cut -c 1-6`
v_depend_day1=`echo $v_depend_procdate1 | cut -c 7-8`
v_depend_month2=`echo $v_depend_procdate2 | cut -c 1-6`
v_depend_day2=`echo $v_depend_procdate2 | cut -c 7-8`
v_depend_month3=`echo $v_depend_procdate3 | cut -c 1-6`
v_depend_day3=`echo $v_depend_procdate3 | cut -c 7-8`
v_depend_month4=`echo $v_depend_procdate4 | cut -c 1-6`
v_depend_day4=`echo $v_depend_procdate4 | cut -c 7-8`
v_depend_month5=`echo $v_depend_procdate5 | cut -c 1-6`

#判断前置依赖

v_cnt1=$(is_depend_success_with_no_prov $v_depend_procdate1 dwa_v_d_cus_al_user_info)
v_cnt2=$(is_depend_success_with_no_prov $v_depend_procdate2 dwa_v_d_cus_al_cust_info)
v_cnt3=$(is_depend_success_with_no_prov $v_depend_procdate3 dwa_v_d_cus_cb_rns_encap)
v_cnt4=$(is_depend_success_with_no_prov $v_depend_procdate4 dwa_v_d_cus_al_rns_encap)
v_cnt5=$(is_depend_success_with_no_prov $v_depend_procdate5 dwa_v_m_cus_al_rns_wide)
echo "v_cnt1="$v_cnt1
echo "v_cnt2="$v_cnt2
echo "v_cnt3="$v_cnt3
echo "v_cnt4="$v_cnt4
echo "v_cnt5="$v_cnt5
#if [ 1 -eq 1 ]; then
if [ $v_cnt1 -eq 1 -a $v_cnt2 -eq 31 -a $v_cnt3 -eq 31 -a $v_cnt4 -eq 31 -a $v_cnt5 -eq 31 ]; then
#定义sql字符?*


v_sql="alter table dm_v_d_cus_user_check drop partition (month_id='"$v_month"',day_id='"$v_day"');"
hive -e "
use $database;
$v_sql
;" 


#定义sql字符?
v_sql="insert overwrite table dm_v_d_cus_user_check partition(month_id = '"$v_month"', day_id = '"$v_day"')
select 
area_no                ,
service_type           ,
device_number          ,
device_number_md5      ,
device_number_sha256   ,
is_lit20			   ,
is_rnet                ,
is_cert_numb5          ,
cert_name_md5          ,
cert_no_md5            ,
cert_name_sha256       ,
cert_no_sha256         ,
cert_type              ,
cert_type_md5          ,
cert_type_sha256       ,
is_card                ,
is_innet               ,
close_date             ,
case
when innet_days < 20 or cert_numbs > 5 or is_rnet = '1' then '1' 
else '0' end is_filter,
status_id_mw           ,
cert_type_mw           ,
case
           when nvl(innet_days, 0) <= 0 then '00'
           when innet_days >= 1 and innet_days <= 25 then '10'
           when innet_days >= 26 and innet_days <= 90 then '20'
           when innet_days >= 91 and innet_days <= 180 then '30'
           when innet_days >= 181 and innet_days <= 365 then '40'
           when innet_days >= 366 and innet_days <= 1095 then '50'
           when innet_days >= 1096 then '60'
         end innet_month_lvl_mw,
case
           when cert_no is null then '00'
           when cert_numbs >= 1 and cert_numbs <= 5 then '01'
           when cert_numbs >= 6 then '06' end cert_numbs_mw,
innet_months           ,
stop_type              ,
innet_date_mw          ,
is_valid_flag          ,
user_id                ,
user_id_md5            ,
user_id_sha256         ,
cert_numbs             ,
cert_innet_usernums    ,
cert_break_usernums    ,
cust_sex               ,
cert_age               ,
pay_mode               ,
zb_channel_no          ,
$v_date,
prov_id,
$v_part,
$v_month,
$v_day,
cust_id				   ,
constellation_desc     
from
(select 
t1.area_no                ,
t1.service_type           ,
t1.device_number          ,
t1.device_number_md5      ,
t1.device_number_sha256   ,
t1.is_rnet                ,
case
when t1.innet_days < 20
then '1' else '0' end is_lit20,
t2.is_cert_numb5          ,
t2.cert_name_md5          ,
t2.cert_no_md5            ,
t3.cert_name_sha256       ,
t3.cert_no_sha256         ,
t2.cert_type              ,
t2.cert_type_md5          ,
t2.cert_type_sha256       ,
t1.is_card                ,
t1.is_innet               ,
t1.close_date             ,                          
t1.status_id_mw           ,
case
           when t2.cert_type = '0101' then
            '00'
           when t2.cert_type = '0102' then
            '01'
           when t2.cert_type = '0103' then
            '02'
           when t2.cert_type = '0104' then
            '03'
           when t2.cert_type = '0105' then
            '04'
           when t2.cert_type = '0106' then
            '05'
           when t2.cert_type = '0107' then
            '06'
           when t2.cert_type = '0108' then
            '07'
           when t2.cert_type = '0199' or substr(t2.cert_type, 1, 2) = '01' then
            '08'
           when t2.cert_type = '0201' then
            '09'
           when t2.cert_type = '0202' then
            '10'
           when t2.cert_type = '0204' then
            '11'
           when t2.cert_type = '0205' then
            '12'
           when t2.cert_type = '0206' then
            '13'
           when t2.cert_type = '0207' then
            '14'
           when t2.cert_type = '0299' or substr(t2.cert_type, 1, 2) = '02' then
            '15'
           else
            '99'
         end cert_type_mw,
t1.innet_months           ,
t1.stop_type              ,
t1.innet_date_mw          ,
t4.is_valid_flag          ,
t1.user_id                ,
t1.user_id_md5            ,
t1.user_id_sha256         ,
t2.cert_numbs             ,
t2.cert_innet_usernums    ,
t2.cert_break_usernums    ,
t3.cust_sex               ,
t3.cert_age               ,
t1.pay_mode               ,
t1.zb_channel_no          ,
t1.prov_id				  ,
t1.innet_days			  ,
t2.cert_no                ,
t1.cust_id                ,
t3.constellation_desc     
from
(
select 
cust_id,
user_id,
upper(md5(user_id)) as user_id_md5,
sha256(user_id) as user_id_sha256,
area_id as  area_no	,
service_type	,
device_number	,
upper(md5(device_number)) as device_number_md5       ,
sha256(device_number)  as device_number_sha256    ,
datediff(
to_date(from_unixtime(unix_timestamp(cast(concat(month_id,day_id) as string),'yyyyMMdd'),'yyyy-MM-dd')),
to_date(from_unixtime(unix_timestamp(cast(innet_date as string),'yyyyMMdd'),'yyyy-MM-dd'))
) + 1 innet_days,

case 
when close_date is null then '0'
when innet_date > close_date and
datediff(
to_date(from_unixtime(unix_timestamp(cast(innet_date as string),'yyyyMMdd'),'yyyy-MM-dd')),
to_date(from_unixtime(unix_timestamp(cast(close_date as string),'yyyyMMdd'), 'yyyy-MM-dd'))
) + 1 < 183 then '1' else '0'
end is_rnet,
is_card,
is_innet,
close_date,
case
       when is_stat = '0' then
        '8'
       when is_innet = '0' then
        '2'
       when is_innet = '1' and stop_type in ('01', '02', '03') then
        '1'
       when is_innet = '1' then
        '0'
     end status_id_mw,
innet_months,
case
 when is_innet = '1' and is_stat = '1' then
 nvl(stop_type, '99')
 else
 '99'
 end stop_type,
innet_date as innet_date_mw,
pay_mode,
channel_id as zb_channel_no,
prov_id 
from zba_dwa.dwa_v_d_cus_al_user_info 
where part_id='$v_depend_month1' and day_id='$v_depend_day1' ) as t1
left join 
(select 
prov_id,
cust_id,
user_id,
CERT_INNET_USERNUMS as  cert_numbs,
case when CERT_INNET_USERNUMS > 5 then '1' else '0' end is_cert_numb5,
cert_innet_usernums,
cert_break_usernums,
cert_no	,
cust_name as cert_name_md5              ,
cert_no  as cert_no_md5                 ,
cert_type                               ,
upper(md5(cert_type)) as cert_type_md5,
sha256(cert_type) as cert_type_sha256
from zba_dwa.dwa_v_d_cus_al_cust_info  
where part_id='$v_depend_month2'  and day_id='$v_depend_day2' and length(cust_id)>5) as t2
on t1.cust_id=t2.cust_id  and t1.user_id=t2.user_id and t1.prov_id=t2.prov_id
left join
(select 
prov_id,
cust_id,
cert_name_sha256,
cert_no_sha256,
cust_sex,
cert_age,
constellation_desc
from 
(
select 
prov_id,
cust_id,
cust_name_sha256 as cert_name_sha256	,
cert_no_sha256							,
cust_sex                                ,
cert_age                                ,
constellation_desc
from zba_dwa.dwa_v_d_cus_cb_rns_encap 
where part_id='$v_depend_month3' and day_id='$v_depend_day3' and length(cust_id)>5  

union all

select 
prov_id,
cust_id,
cust_name_sha256 as cert_name_sha256    ,
cert_no_sha256							,
cust_sex                                ,
cert_age                                ,
constellation_desc
from zba_dwa.dwa_v_d_cus_al_rns_encap 
where part_id='$v_depend_month4' and day_id='$v_depend_day4' and length(cust_id)>5 
)tmp group by 
prov_id,cust_id,cert_name_sha256,cert_no_sha256,cust_sex,cert_age,constellation_desc) as t3 
on t1.cust_id=t3.cust_id and t1.prov_id=t3.prov_id
left join  
(select 
prov_id,
cust_id,
is_valid_flag  
from zba_dwa.dwa_v_m_cus_al_rns_wide
where month_id='$v_depend_month5' and length(cust_id)>5  group by prov_id,cust_id,is_valid_flag) as t4 
on t1.cust_id=t4.cust_id and t1.prov_id=t4.prov_id
) as t
"
#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $database;
set mapred.job.name=risk_control@p_dm_v_d_cus_user_check_$v_month_$v_day;
add jar ../jar/hive_encrypt_udf.jar;
create temporary function sha256 as 'com.unicom.hive.udf.SHA256';
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
v_sql="alter table dm_v_d_cus_user_check drop partition (month_id='"$v_7month"',day_id='"$v_7day"');"
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
