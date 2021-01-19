package com.unicom.entity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LogAccoutInfo {
    private int tableId      ;
    private int column_id;
    private String accountPeriod;
    private String cycle          ;

    public LogAccoutInfo() {
    }

    public LogAccoutInfo(int tableId, int column_id, String accountPeriod) {
        this.tableId = tableId;
        this.column_id = column_id;
        this.accountPeriod = accountPeriod;
        if(accountPeriod.length()==6){
            this.cycle="M";
        }else{
            this.cycle="D";
        }
    }

    public int getTableId() {
        return tableId;
    }

    public int getColumn_id() {
        return column_id;
    }

    public String getAccountPeriod() {
        return accountPeriod;
    }

    public String getCycle() {
        return cycle;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setColumn_id(int column_id) {
        this.column_id = column_id;
    }

    public void setAccountPeriod(String accountPeriod) {
        this.accountPeriod = accountPeriod;
    }

    public void setCycle(String cycle) {
        this.cycle = cycle;
    }

    @Override
    public String toString() {
        return "LogAccoutInfo{" +
                "tableId=" + tableId +
                ", column_id=" + column_id +
                ", accountPeriod='" + accountPeriod + '\'' +
                ", cycle='" + cycle + '\'' +
                '}';
    }
}
