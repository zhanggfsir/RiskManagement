#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hbase
# *名称         --%@NAME:           import_data_slaver.sh
# *功能描述     --%@COMMENT:      	HBase入备库
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          日模型或月模型入库 day_id 或 month_id
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *参数         --%@PARAM:          nextval 
# *参数         --%@PARAM:          serv_type 例：user_check
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-03-24
# *层次         --%@LEVEL:          DWA
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *来源表       --%@FROM:           ubd_risk_serv.dm_v_d_cus_device_reuse_risk   
# *来源表       --%@FROM:			ubd_risk_serv.dm_v_d_use_mb_voice_payeco_risk
# *来源表       --%@FROM:			ubd_risk_serv.dm_v_d_use_cb_voice_payeco_risk
# *来源表       --%@FROM:			ubd_risk_serv.dm_v_d_cus_user_check
# *目标表       --%@TO:             re_down_use     
# *目标表       --%@TO:				payeco_position_new
# *目标表       --%@TO:				user_check user_check_new
# *目标表       --%@TO:				re_down_tera
# *目标表       --%@TO:				re_down_innet
# *目标表       --%@TO:				serv_channel_no
###############################################################################
# 调用方法: bash import_data_slave.sh day_id 20161230  220120430 ant_serv
###############################################################################
# 函数引用，使用相对路径

.  ./p_pub_func_log_mysql.sh

###############################################################################
# 环境变量设置

export JAVA_HOME=/data/ubd_serv_risk/jdk1.8.0_181
export CLASSPATH=.:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
export LOG_HOME=/data/log

###############################################################################
# 声明变量,变量赋值
info=`cat database.conf`
#echo "info="$info
user=`echo $info |awk -F: '{print $1}'`     #数据库用户
password=`echo $info |awk -F: '{print $2}'` #数据库密码
database=`echo $info |awk -F: '{print $3}'` #数据库名称

echo "user="$user

v_schema_table_name=`echo $1|tr [A-Z] [a-z]`
v_table_name=${v_schema_table_name##*.}  #删除点号左边的并拼表名
v_schema_name=`echo ${v_schema_table_name} | cut -d . -f 1`
v_proc_name="p_"$v_table_name
v_condition=$2 
v_condition_value=$3
v_nextval=$4
v_serv_code=$5
v_env=$6
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

v_type=`expr substr $2 1 1|tr 'a-z' 'A-Z'` #判断日还是月
monthId=`echo ${v_condition_value} |awk '{print substr($1,1,6)}'`
dayId=`echo ${v_condition_value} |awk '{print substr($1,7,2)}'`
partId=$((monthId%2))
###############################################################################
#获取前两期日期（数据文件只保留2个账期数据）
if [ $v_type = 'D' ]; then
  type=day
  v_condition_value_bef2=`date +%Y%m%d -d "$v_condition_value -2 day"`
else
  type=month
  v_condition_value_d=${v_condition_value}01
  v_condition_value_bef2=`date +%Y%m -d "$v_condition_value_d -2 month"`
fi

###获取参数###
select_sql="select concat(coalesce(concat(file_id,'A'),''),':',
       coalesce(serv_cycle ,''),':',
       coalesce(lower(local_database) ,''),':',
       coalesce(lower(oppose_database) ,''),':',
       coalesce(concat(substr(serv_code,1,instr(serv_code,'_')-1),substr(serv_code,instr(serv_code,'_')+1)),''),':',
       coalesce(tar_hbase_tablename ,''),':', 
       coalesce(tar_hbase_cycle ,''),':',
       coalesce(hbase_create_xml ,''),':',
       coalesce(hbase_insert_xml ,''),':',
       coalesce(hbase_drop_xml ,''),':',
       coalesce(hbase_major_xml ,''),':',
       coalesce(hbasebak_create_xml ,''),':',
       coalesce(hbasebak_insert_xml ,''),':',
       coalesce(hbasebak_drop_xml ,''),':',
       coalesce(hbasebak_major_xml ,''),':',
       coalesce(is_hdfs_imphbase ,''),':',
       coalesce(thread_imphbase ,''),':',
       coalesce(hbasetable_effect_cycle ,0))
  from lf_zx_mysql.${cfg_sla_tb} t
 where t.is_valid= '1'
   and upper(t.serv_cycle) = upper('${v_type}')
   and upper(t.table_name )=upper('${v_schema_table_name}')
   and upper(t.serv_code)=upper('${v_serv_code}');"
v_interface_code_str1=$(log_mysql)
###获取参数###
echo "v_interface_code_str1="$v_interface_code_str1
v_file_id_a=`echo ${v_interface_code_str1} |awk -F: '{ print $1}'` #文件名称A文件
v_serv_cycle=`echo ${v_interface_code_str1} |awk -F: '{ print $2}'` #服务周期
v_local_database=`echo ${v_interface_code_str1} |awk -F: '{ print $3}'` #本端数据库标识
v_oppose_database=`echo ${v_interface_code_str1} |awk -F: '{ print $4}'` #目标数据库标识
v_file_keycode=`echo ${v_interface_code_str1} |awk -F: '{ print $5}'` #服务编码
v_tar_hbase_tablename=`echo ${v_interface_code_str1} |awk -F: '{ print $6}'` #HBase数据表名
v_tar_hbase_cycle=`echo ${v_interface_code_str1} |awk -F: '{ print $7}'` #HBase数据表周期
v_hbase_create_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $8}'` #HBase主库-创建表配置文件
v_hbase_insert_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $9}'` #HBase主库-插入表配置文件
v_hbase_drop_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $10}'` #HBase主库-删除表配置文件
v_hbase_major_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $11}'` #HBase主库-合并表配置文件
v_hbasebak_create_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $12}'` #HBase备库-创建表配置文件
v_hbasebak_insert_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $13}'` #HBase备库-插入表配置文件
v_hbasebak_drop_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $14}'` #HBase备库-删除表配置文件
v_hbasebak_major_xml=`echo ${v_interface_code_str1} |awk -F: '{ print $15}'` #HBase备库-合并表配置文件
v_is_hdfs_imphbase=`echo ${v_interface_code_str1} |awk -F: '{ print $16}'` #HBase入库文件是否为HDFS
v_thread_imphbase=`echo ${v_interface_code_str1} |awk -F: '{ print $17}'` #HBsae入库时启动的线程数
v_hbasetable_effect_cycle=`echo ${v_interface_code_str1} |awk -F: '{ print $18}'` #HBase数据表的有效周期（数据表存储周期）

if [ "$v_is_hdfs_imphbase" == "0" ]; then
  is_hdfs=false
else
  is_hdfs=true
fi

###获取文件名###
if [ $v_table_name == "dm_v_d_use_mb_voice_payeco_risk" -o $v_table_name == "dm_v_d_use_cb_voice_payeco_risk" ];then
    v_interface_name=/user/${hbaseDB}/${database}.db/${v_table_name}/part_id=${partId}/day_id=${dayId}
else
    v_interface_name=/user/${hbaseDB}/${database}.db/${v_table_name}/month_id=${monthId}/day_id=${dayId}
fi
###获取HBase实体表名###
if [ "$v_tar_hbase_cycle" == "M" ];then
  ##本期HBase实体表名##
  v_tabname=$v_tar_hbase_tablename$monthId #账期 本期插入表名
  #echo "v_tabname="$v_tabname
  ##上上期HBase实体表，后期删除操作##
  v_omonth=`date +%Y%m -d "$3 -$v_hbasetable_effect_cycle month"` #获取创建表的账期
  v_tabnamem_old=$v_hb_table_name$v_omonth #本期创建表名
  #echo "v_tabnamem_old="$v_tabnamem_old
else
  ##本期HBase实体表名##
  v_tabname=$v_tar_hbase_tablename$v_condition_value #账期 本期插入表名
  #echo "v_tabname="$v_tabname
  ##上上期HBase实体表，后期删除操作##
  v_omonth=`date +%Y%m%d -d "$3 -$v_hbasetable_effect_cycle day"` #获取创建表的账期
  v_tabnamed_old=$v_hb_table_name$v_omonth #本期创建表名
  #echo "v_tabnamed_old="$v_tabnamed_old
fi


  ###HBase创建表###
  ###由于数据源的存储周期类型与目标表的存储周期类型不一致需要做分支判断，暂分为3种组合情况：来源为D、目标为M；来源为D、目标为D；来源为M、目标为M##
  ##源数据为日表而目标数据表为月表,每月1日创建1次，本期数据成功入库1省后创建下期实体表##
  #echo "创建表"
if [ "$v_serv_cycle" = 'D' -a "$v_tar_hbase_cycle" = 'M' -a "$dayId" = '01' ]; then
    ##备库##
  java -Xmx1024m -cp ../jar/HBase_Tool.jar createTable.CreateTable ../config/hbase/slaver/createTable/${v_hbasebak_create_xml} ${v_tabname}
    #echo "1"
fi

  ##源数据为日表而目标数据表为日表,本期数据成功入库1省后创建下期实体表##
if [ "$v_serv_cycle" = 'D' -a "$v_tar_hbase_cycle" = 'D' ]; then
    ##备库##
  java -Xmx1024m -cp ../jar/HBase_Tool.jar createTable.CreateTable ../config/hbase/slaver/createTable/${v_hbasebak_create_xml} ${v_tabname}
    #echo "2"
fi

  ##源数据为月表而目标数据表为月表,本期数据成功入库1省后创建下期实体表##
if [ "$v_serv_cycle" = 'M' -a "$v_tar_hbase_cycle" = 'M' ]; then
    ##备库##
    java -Xmx1024m -cp ../jar/HBase_Tool.jar createTable.CreateTable ../config/hbase/slaver/createTable/${v_hbasebak_create_xml} ${v_tabname}
fi
###更新日志###
#select_sql="update lf_zx_mysql.LOG_SERV_DATA_INTERACT_SLAVE set tar_tablename=null,load_startime=null,load_endtime=null,load_database_rows=0,load_databasebak_rows=0,is_success_load='0' where sortid='${v_nextval}' and lower(serv_code)=lower('${v_serv_code}') and acct_date='${v_condition_value}' and prov_id='${provId}' and lower(src_tablename)=lower('${v_schema_table_name}');"
#v_log_update_sql=$(log_mysql)
v_logfile=$LOG_HOME/serv/risk/$type/$v_condition_value/${v_proc_name}_${v_nextval}
v_logfile_dir=$LOG_HOME/serv/risk/$type/$v_condition_value
if [ ! -d $v_logfile_dir ];then
mkdir -p $v_logfile_dir
fi
###HBase入库###
##备库##
java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/slaver/insertTable/${v_hbasebak_insert_xml} ${v_interface_name} ${v_tabname} ${v_thread_imphbase}  2>&1 |tee $v_logfile"_bak.log" >>/dev/null
echo $execlog_name
#hbase入库完成时间
v_hbase_endtime=`echo $(date "+%Y%m%d%H%M%S")`
#echo "v_hbase_endtime="$v_hbase_endtime
log_date=`date +%Y%m%d`
execlog_name=$v_logfile'_bak.log'
#查看HBase备库成功入库记录数
#unload_hbase_rows=`cat ../exec_log/${execlog_name} |grep /hbase_cx, |awk -F: '{print $4}' |awk -F, '{print $1}'|awk 'BEGIN{sum=0}{sum+=$1}END{print sum}'`
#select_sql="update lf_zx_mysql.DIM_SERV_DATA_WEBSERVICE set table_name='${v_tabname}',update_time=now()  where lower(substr(table_name,1,length(table_name)-length('${v_condition_value}')))=lower('${v_tar_hbase_tablename}');"
#v_log_update_sql=$(log_mysql)
###############################################################################
# 更新日志
# 固定模板，不需修改
#select_sql="insert into lf_zx_mysql.DIM_SERV_DATA_WEBSERVICE_LOG (product_id,module_name,table_name,date_type,update_time) values ('1','${v_schema_table_name}','${v_tabname}','${v_type}','${update_time}');"
#$(log_mysql)
###############################################################################