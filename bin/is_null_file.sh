#!/bin/bash
#
###############################################################################
# *脚本类型     --%@TYPE:
# *名称         --%@NAME:           is_null_file.sh
# *功能描述     --%@COMMENT:         判断目标路径下文件是否为空
# *执行周期     --%@PERIOD:          D
# *参数         --%@PARAM:          帐期 YYYYMMDD
# *创建人       --%@CREATOR:        王玮
# *创建时间     --%@CREATED_TIME:    2020-02-02
# *层次         --%@LEVEL:          RISK
# *数据域       --%@DOMAIN:         风控
# *备注         --%@REMARK:
# *修改记录     --%@MODIFY:
###############################################################################
# 调用方法: sh is_null_file.sh 20200202 DHB_CSF/DHB_XYF
###############################################################################
#脚本输入参数
v_date=$1
v_month=`echo $v_date | cut -c 1-6`
v_day=`echo $v_date | cut -c 7-8`
v_serv_code=$2
###############################################################################
###获取文件名###
if [ $v_serv_code == "DHB_CSF" ];then
    v_file_location=/user/lf_zh_pro/lf_dhb_pro/cu_csfx_v2/csfx/result/month_id=${v_month}/day_id=${v_day}
else
    v_file_location=/user/lf_zh_pro/lf_dhb_pro/cu_csfx_v2/calllog7/result/month_id=${v_month}/day_id=${v_day}
fi
#判断目录下文件是否为空
hadoop fs -ls  ${v_file_location} >> /dev/null 2>&1
echo $?