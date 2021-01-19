#!/bin/bash

table_name=$1
current_date=`date +%Y%m%d`
echo ${current_date}

v_month=`echo $current_date | cut -c 1-6`
v_date=`echo $current_date | cut -c 1-8`
v_day=`echo $current_date | cut -c 7-8`
echo $v_month $v_date $v_day 

daily_path=/data/log/serv/risk/day/${v_date}/
monthly_path=/data/log/serv/risk/month/${v_month}/

if [ ! -d $daily_path ];then
mkdir -p $daily_path
fi

if [ ! -d $monthly_path ];then
mkdir -p $monthly_path
fi


java -Xmx4096m -cp ../jar/risk-1.0-jar-with-dependencies.jar com.unicom.tools.MajorCompactTable 319 ${table_name} >>${daily_path}/319_majorCompact_319_${table_name}.log
java -Xmx4096m -cp ../jar/risk-1.0-jar-with-dependencies.jar com.unicom.tools.MajorCompactTable 419 ${table_name} >>${daily_path}/419_majorCompact_319_${table_name}.log

