#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hbase
# *名称         --%@NAME:           import_data_slaver_personal.sh
# *功能描述     --%@COMMENT:      	个性化接口入备库
# *执行周期     --%@PERIOD:         D/M
# *参数         --%@PARAM:          日模型或月模型入库 day_id 或 month_id
# *参数         --%@PARAM:          serv_type 例：user_check
# *参数         --%@PARAM:          date 账期 例：20190720
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-07-09
# *层次         --%@LEVEL:          
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:         
# *来源表       --%@FROM:              
# *目标表       --%@TO:                  
###############################################################################
# 调用方法: bash import_data_slaver_personal.sh day_id 20161230  dxm
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

# ?SQLPLUS?
SQLSET="set echo off\nset head off\nset pagesize 0\nset linesize 2000\nset heading off\nset trimspool on\nset feedback off\nset term off\n"


v_condition=$1 
v_condition_value=$2
v_serv_code=$3
v_month=`echo $2 | cut -c 1-6`
v_day=`echo $2 | cut -c 7-8`

###############################################################################
#获取前两期日期（数据文件只保留2个账期数据）


###获取参数###
select_sql="select 
       coalesce(file_location,''),':',
       coalesce(tar_tablename,''),':', 
       coalesce(data_type,''),':',
       coalesce(create_xml,''),':',
       coalesce(insert_xml,''),':',
       coalesce(drop_xml,''),':',
       coalesce(create_xml_bak,''),':',
       coalesce(insert_xml_bak,''),':',
       coalesce(drop_xml_bak,'')
  from lf_zx_mysql.DIM_SERV_DATA_PERSONAL_DB t
   where upper(t.serv_code)=upper('${v_serv_code}');"
v_interface_code_str1=$(log_mysql)
###获取参数###
echo "v_interface_code_str1="$v_interface_code_str1
v_file_location=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $1}'` #文件位置
v_tar_tablename=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $2}'` #hbase表名
v_data_type=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $3}'` #周期
v_create_xml=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $4}'` #HBase主库-创建表配置文件
v_insert_xml=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $5}'` #HBase主库-插入表配置文件
v_drop_xml=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $6}'` #HBase主库-删除表配置文件
v_create_xml_bak=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $7}'` #HBase备库-创建表配置文件
v_insert_xml_bak=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $8}'` #HBase备库-插入表配置文件
v_drop_xml_bak=`echo ${v_interface_code_str1} |awk -F ' : ' '{ print $9}'` #HBase备库-删除表配置文件
v_thread_number='5'


###获取文件名###
if [ $v_serv_code == "JL_SCORE" -o $v_serv_code == "GX_LX" ];then
    v_interface_name=${v_file_location}/month_id=${v_month}/
elif [ $v_serv_code == "DHB_CSF" -o $v_serv_code == "DHB_XYF" ];then
    v_interface_name=${v_file_location}/month_id=${v_month}/day_id=${v_day}/
else
    v_interface_name=${v_file_location}${v_condition_value}
fi

#HBase表名
v_tabname=${v_tar_tablename}${v_condition_value}
#创建表
java -Xmx1024m -cp ../jar/HBase_Tool.jar createTable.CreateTable ../config/hbase/slaver/createTable/${v_create_xml_bak} ${v_tabname}


v_logfile=$LOG_HOME/serv/risk/$type/$v_condition_value/$v_tabname
v_logfile_dir=$LOG_HOME/serv/risk/$type/$v_condition_value
if [ ! -d $v_logfile_dir ];then
mkdir -p $v_logfile_dir
fi
###HBase入库###
##主库##
java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/slaver/insertTable/${v_insert_xml_bak} ${v_interface_name} ${v_tabname} ${v_thread_number} 2>&1 |tee $v_logfile".log" >>/dev/null

execlog_name=$v_logfile'.log'
echo $execlog_name

###############################################################################
# 更新日志
# 固定模板，不需修改

###############################################################################
