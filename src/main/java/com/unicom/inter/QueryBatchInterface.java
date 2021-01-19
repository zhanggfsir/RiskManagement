package com.unicom.inter;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public interface QueryBatchInterface {
    /**
     为了验证数据的正确性，从HDFS上获取数据，并从HBASE中查得该数据；分别写出到HDFS。
     */
    void put2Hdfs(FileSystem fs, LoadColumnInfo loadColumnInfo, HashSet<String> lineSet, String tableName, String account,
                         FSDataOutputStream hiveOutputStream,FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException ;

    /**
     * 从HDFS上随机获取1条数据
     */
    HashMap<String,String>  getFieldFromHdfs(String linePath, String[] arrayField,HashMap<String,String> hdfsMap,String account) throws IOException;

    /**
        从Hbase中获得数据并写出到HDFS
     */
    void getDataFromHbaseAndWrite(ZkInfo zkInfo,String tableName,String familyName,String columnName,
                                                 HashMap<String,String> hdfsMap,FSDataOutputStream hiveOutputStream,FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException ;
    }

