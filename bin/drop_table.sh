#!/bin/bash
#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:           hbase
# *名称         --%@NAME:           drop_table.sh
# *功能描述     --%@COMMENT:      	删除HbaseT-9天的表
# *执行周期     --%@PERIOD:         D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:
# *创建时间     --%@CREATED_TIME:   2019-04-17
# *层次         --%@LEVEL:          
# *数据域       --%@DOMAIN:         
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:       	
# *来源表       --%@FROM:           
# *目标表       --%@TO:             
###############################################################################
# 调用方法: bash drop_table.sh 20190417 
###############################################################################

data=$1
#v_habseName=$2
#v_start=`date +%Y%m%d -d "$data -7 day"`
#v_end=`date +%Y%m%d -d "$data -15 day"`
t1="$v_start"
t2="$v_end"
tablename=("re_down_use" "payeco_position_new" "user_check" "user_check_new" "re_down_tera" "re_down_innet" "serv_channel_no")

for table in ${tablename[@]} ; 
	do	
	v_end=`date +%Y%m%d -d "$data -10 day"`
	v_start=`date +%Y%m%d -d "$data -9 day"`
		while [[ ${v_end} < ${v_start} ]];
		    do 
			v_end=`date +%Y%m%d -d "$v_end +1 day"`
			echo ${table}${v_end}
			java -Xmx2048m -cp ../jar/HBase_Tool.jar dropTable.DropTable  ../config/hbase/master/dropTable/drop-table-config-all-tables.xml ${table}${v_end}
			java -Xmx2048m -cp ../jar/HBase_Tool.jar dropTable.DropTable  ../config/hbase/slaver/dropTable/drop-table-config-all-tables-bak.xml ${table}${v_end}
		done
	done

