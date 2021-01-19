#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hbase
# *名称         --%@NAME:           import_data_interface_log.sh
# *功能描述     --%@COMMENT:      	HBase入库
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          yyyyMMdd
# *参数         --%@PARAM:          nextval
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-07-25
# *层次         --%@LEVEL:          DM
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *来源表       --%@FROM:			/files/ubd_xl_cp/risk/interface_log/
# *目标表       --%@TO:				interface_log
###############################################################################
# 调用方法: bash import_data_interface_log.sh yyyyMMdd
###############################################################################
# 函数引用，使用相对路径

.  ./p_pub_func_log_mysql.sh
###############################################################################
# 环境变量设置
v_date=$1
v_file=/files/ubd_xl_cp/risk/interface_log/interface_log_${v_date}.txt

export JAVA_HOME=/data/ubd_serv_risk/jdk1.8.0_181
export CLASSPATH=.:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/dt.jar:JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
export LOG_HOME=/data/log

###HBase入库###
##主库##
java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/master/insertTable/insert-table-config-interface-log.xml $v_file interface_log 1 2>&1
##备库##
java -Xmx4096m -cp ../jar/HBase_Tool.jar insertTable.InsertTableFromHdfs ../config/hbase/slaver/insertTable/insert-table-config-interface-log-bak.xml $v_file interface_log 1 2>&1
