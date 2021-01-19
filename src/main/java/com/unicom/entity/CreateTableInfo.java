package com.unicom.entity;

public class CreateTableInfo {
    private int tableId;
    private String tableName;
    private int tableType;
    private int interfaceType;
    private int maxVersions;
    private int minVersions;
    private int blockSize;
    private boolean blockCacheEnabled;
    private boolean inMemory;
    private int timeToLive;
    private String bloomFilterType;
    private String compressionType;
    private boolean cacheBloomsOnWrite;
    private boolean cacheDataOnWrite;
    private boolean cacheIndexesOnWrite;
    private boolean compressTags;
    private int scope;
    private boolean keepDeletedCells;
    private String dataBlockEncoding;
    private int regionNum;
    private int startKey;
    private  int endKey;
    private String  familyName;
    private String  creator;
    private String  tableDesc;
    private String mask;

    public CreateTableInfo() {
    }


    public CreateTableInfo(int tableId, String tableName, int tableType, int interfaceType, int maxVersions, int minVersions, int blockSize, boolean blockCacheEnabled, boolean inMemory, int timeToLive, String bloomFilterType, String compressionType, boolean cacheBloomsOnWrite, boolean cacheDataOnWrite, boolean cacheIndexesOnWrite, boolean compressTags, int scope, boolean keepDeletedCells, String dataBlockEncoding, int regionNum, int startKey, int endKey, String familyName, String creator, String tableDesc, String mask) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.tableType = tableType;
        this.interfaceType = interfaceType;
        this.maxVersions = maxVersions;
        this.minVersions = minVersions;
        this.blockSize = blockSize;
        this.blockCacheEnabled = blockCacheEnabled;
        this.inMemory = inMemory;
        this.timeToLive = timeToLive;
        this.bloomFilterType = bloomFilterType;
        this.compressionType = compressionType;
        this.cacheBloomsOnWrite = cacheBloomsOnWrite;
        this.cacheDataOnWrite = cacheDataOnWrite;
        this.cacheIndexesOnWrite = cacheIndexesOnWrite;
        this.compressTags = compressTags;
        this.scope = scope;
        this.keepDeletedCells = keepDeletedCells;
        this.dataBlockEncoding = dataBlockEncoding;
        this.regionNum = regionNum;
        this.startKey = startKey;
        this.endKey = endKey;
        this.familyName = familyName;
        this.creator = creator;
        this.tableDesc = tableDesc;
        this.mask = mask;
    }

    public int getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableType() {
        return tableType;
    }

    public int getInterfaceType() {
        return interfaceType;
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public int getMinVersions() {
        return minVersions;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public boolean isBlockCacheEnabled() {
        return blockCacheEnabled;
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public String getBloomFilterType() {
        return bloomFilterType;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public boolean isCacheBloomsOnWrite() {
        return cacheBloomsOnWrite;
    }

    public boolean isCacheDataOnWrite() {
        return cacheDataOnWrite;
    }

    public boolean isCacheIndexesOnWrite() {
        return cacheIndexesOnWrite;
    }

    public boolean isCompressTags() {
        return compressTags;
    }

    public int getScope() {
        return scope;
    }

    public boolean isKeepDeletedCells() {
        return keepDeletedCells;
    }

    public String getDataBlockEncoding() {
        return dataBlockEncoding;
    }

    public int getRegionNum() {
        return regionNum;
    }

    public int getStartKey() {
        return startKey;
    }

    public int getEndKey() {
        return endKey;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getCreator() {
        return creator;
    }

    public String getTableDesc() {
        return tableDesc;
    }

    public String getMask() {
        return mask;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableType(int tableType) {
        this.tableType = tableType;
    }

    public void setInterfaceType(int interfaceType) {
        this.interfaceType = interfaceType;
    }

    public void setMaxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
    }

    public void setMinVersions(int minVersions) {
        this.minVersions = minVersions;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public void setBlockCacheEnabled(boolean blockCacheEnabled) {
        this.blockCacheEnabled = blockCacheEnabled;
    }

    public void setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    public void setBloomFilterType(String bloomFilterType) {
        this.bloomFilterType = bloomFilterType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public void setCacheBloomsOnWrite(boolean cacheBloomsOnWrite) {
        this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    }

    public void setCacheDataOnWrite(boolean cacheDataOnWrite) {
        this.cacheDataOnWrite = cacheDataOnWrite;
    }

    public void setCacheIndexesOnWrite(boolean cacheIndexesOnWrite) {
        this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    }

    public void setCompressTags(boolean compressTags) {
        this.compressTags = compressTags;
    }

    public void setScope(int scope) {
        this.scope = scope;
    }

    public void setKeepDeletedCells(boolean keepDeletedCells) {
        this.keepDeletedCells = keepDeletedCells;
    }

    public void setDataBlockEncoding(String dataBlockEncoding) {
        this.dataBlockEncoding = dataBlockEncoding;
    }

    public void setRegionNum(int regionNum) {
        this.regionNum = regionNum;
    }

    public void setStartKey(int startKey) {
        this.startKey = startKey;
    }

    public void setEndKey(int endKey) {
        this.endKey = endKey;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public void setTableDesc(String tableDesc) {
        this.tableDesc = tableDesc;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }

    @Override
    public String toString() {
        return "CreateTableInfo{" +
                "tableId=" + tableId +
                ", tableName='" + tableName + '\'' +
                ", tableType=" + tableType +
                ", interfaceType=" + interfaceType +
                ", maxVersions=" + maxVersions +
                ", minVersions=" + minVersions +
                ", blockSize=" + blockSize +
                ", blockCacheEnabled=" + blockCacheEnabled +
                ", inMemory=" + inMemory +
                ", timeToLive=" + timeToLive +
                ", bloomFilterType='" + bloomFilterType + '\'' +
                ", compressionType='" + compressionType + '\'' +
                ", cacheBloomsOnWrite=" + cacheBloomsOnWrite +
                ", cacheDataOnWrite=" + cacheDataOnWrite +
                ", cacheIndexesOnWrite=" + cacheIndexesOnWrite +
                ", compressTags=" + compressTags +
                ", scope=" + scope +
                ", keepDeletedCells=" + keepDeletedCells +
                ", dataBlockEncoding='" + dataBlockEncoding + '\'' +
                ", regionNum=" + regionNum +
                ", startKey=" + startKey +
                ", endKey=" + endKey +
                ", familyName='" + familyName + '\'' +
                ", creator='" + creator + '\'' +
                ", tableDesc='" + tableDesc + '\'' +
                ", mask='" + mask + '\'' +
                '}';
    }
}
