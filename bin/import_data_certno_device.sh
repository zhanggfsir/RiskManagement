#!/bin/bash

export JAVA_HOME=/data/ubd_serv_risk/jdk1.8.0_181
export CLASSPATH=.:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
export LOG_HOME=/data/log
v_date=$1
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
v_table_name=certno_device
update_q=q

v_tablename=${v_table_name}${v_date}
v_logfile_dir=$LOG_HOME/serv/risk/day/$v_date/
if [ ! -d $v_logfile_dir ];then
mkdir -p $v_logfile_dir
fi

#java -Xmx1024m -cp ../jar/HBase_Tool.jar createTable.CreateTable ../config/hbase/master/createTable/create-table-config-certno-device.xml ${v_table_name}

java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/slaver/insertTable/insert-table-config-certno-device-bak.xml /user/${hbaseDB}/${database}.db/dim_d_cus_certno_device/month_id=$v_month/day_id=$v_day/ ${v_table_name} 4  2>&1 |tee ${v_logfile_dir}${v_tablename}"bak.log" >>/dev/null

java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/master/insertTable/insert-table-config-certno-device.xml /user/${hbaseDB}/${database}.db/dim_d_cus_certno_device/month_id=$v_month/day_id=$v_day/ ${v_table_name} 4  2>&1 |tee ${v_logfile_dir}${v_tablename}".log" >>/dev/null


#点查询系统更新账期 向前端发送账期
nohup curl -v "http://10.244.11.76:9080/api/accountPeriod/updateHbasePeriod" -H "Content-Type: application/json" -d '
{
  "tableName":"'"$v_table_name"'",
  "columnName":"'"$update_q"'",
  "period":"'"$v_date"'" 
}'  >>${v_logfile_dir}${v_tablename}".log" 2>&1 >>/dev/null &



