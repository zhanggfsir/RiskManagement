package com.unicom.entity;
import com.unicom.utils.JdbcUtil;

import java.sql.*;

public class ZkInfo {
    private int zkId       ;
    private String zkName  ;
    private String zkQuorum ;
    private String zkParent;
    private String zkDesc;
    private String mask      ;

//   TODO 为了在数据 查询 时保持与原来一致，新增2个字段，后续是否真正使用待定
    private boolean isCache;
    private int maxVersion;

    @Override
    public String toString() {
        return "ZkInfo{" +
                "zkId=" + zkId +
                ", zkName='" + zkName + '\'' +
                ", zkQuorum='" + zkQuorum + '\'' +
                ", zkParent='" + zkParent + '\'' +
                ", zkDesc='" + zkDesc + '\'' +
                ", mask='" + mask + '\'' +
                '}';
    }

    public int getZkId() {
        return zkId;
    }

    public String getZkName() {
        return zkName;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getZkParent() {
        return zkParent;
    }

    public String getZkDesc() {
        return zkDesc;
    }

    public String getMask() {
        return mask;
    }

    public void setZkId(int zkId) {
        this.zkId = zkId;
    }

    public void setZkName(String zkName) {
        this.zkName = zkName;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public void setZkParent(String zkParent) {
        this.zkParent = zkParent;
    }

    public void setZkDesc(String zkDesc) {
        this.zkDesc = zkDesc;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }

    public boolean isCache() {
        return false;
    }

    public int getMaxVersion() {
        return 1000;
    }

    public void setCache(boolean cache) {
        isCache = cache;
    }

    public void setMaxVersion(int maxVersion) {
        maxVersion = maxVersion;
    }
}

