#!/bin/bash
#source config/config_server.conf
user=$1
password=$2
database=$3
sql=$4
sqlplus -S $user/$password@$database<< SQLEOF
set heading off 
set pagesize 0; 
set feedback off; 
set verify off; 
set echo off; 
set linesize 5000
$sql
exit; 
SQLEOF
