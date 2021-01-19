package com.unicom.entity;

public class LoadColumnInfo {
    private int columnId        ;
    private int tableId         ;
    private String columnName   ;
    private String columnDesc   ;
    private boolean isHdfs      ;
    private String nameService  ;
    private String filePath     ;
    private String  partitions;
    private String seperator     ;
    private int fieldNum        ;
    private String familyName   ;
    private String compressType ;
    private int threadNum       ;
    private String jarName  ;
    private String className     ;
    private String queryClassName;
    private String creator       ;
    private String mask          ;

    public LoadColumnInfo() {
    }

    public LoadColumnInfo(int columnId, int tableId, String columnName, String columnDesc, boolean isHdfs, String nameService, String filePath, String partitions, String seperator, int fieldNum, String familyName, String compressType, int threadNum, String jarName, String className, String queryClassName, String creator, String mask) {
        this.columnId = columnId;
        this.tableId = tableId;
        this.columnName = columnName;
        this.columnDesc = columnDesc;
        this.isHdfs = isHdfs;
        this.nameService = nameService;
        this.filePath = filePath;
        this.partitions = partitions;
        this.seperator = seperator;
        this.fieldNum = fieldNum;
        this.familyName = familyName;
        this.compressType = compressType;
        this.threadNum = threadNum;
        this.jarName = jarName;
        this.className = className;
        this.queryClassName = queryClassName;
        this.creator = creator;
        this.mask = mask;
    }

    public void setQueryClassName(String queryClassName) {
        this.queryClassName = queryClassName;
    }

    public String getQueryClassName() {
        return queryClassName;
    }

    public String getJarName() {
        return jarName;
    }

    public String getClassName() {
        return className;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getPartitions() {
        return partitions;
    }

    public void setHdfs(boolean hdfs) {
        isHdfs = hdfs;
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    public int getColumnId() {
        return columnId;
    }

    public int getTableId() {
        return tableId;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnDesc() {
        return columnDesc;
    }

    public boolean isHdfs() {
        return isHdfs;
    }

    public String getNameService() {
        return nameService;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getSeperator() {
        return seperator;
    }

    public int getFieldNum() {
        return fieldNum;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getCompressType() {
        return compressType;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public String getCreator() {
        return creator;
    }

    public String getMask() {
        return mask;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setColumnDesc(String columnDesc) {
        this.columnDesc = columnDesc;
    }

    public void setIsHdfs(boolean hdfs) {
        isHdfs = hdfs;
    }

    public void setNameService(String nameService) {
        this.nameService = nameService;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setSeperator(String seperator) {
        this.seperator = seperator;
    }

    public void setFieldNum(int fieldNum) {
        this.fieldNum = fieldNum;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }

    @Override
    public String toString() {
        return "LoadColumnInfo{" +
                "columnId=" + columnId +
                ", tableId=" + tableId +
                ", columnName='" + columnName + '\'' +
                ", columnDesc='" + columnDesc + '\'' +
                ", isHdfs=" + isHdfs +
                ", nameService='" + nameService + '\'' +
                ", filePath='" + filePath + '\'' +
                ", partitions='" + partitions + '\'' +
                ", seperator='" + seperator + '\'' +
                ", fieldNum=" + fieldNum +
                ", familyName='" + familyName + '\'' +
                ", compressType='" + compressType + '\'' +
                ", threadNum=" + threadNum +
                ", jarName='" + jarName + '\'' +
                ", className='" + className + '\'' +
                ", creator='" + creator + '\'' +
                ", mask='" + mask + '\'' +
                ", queryClassName='" + queryClassName + '\'' +
                '}';
    }
}
