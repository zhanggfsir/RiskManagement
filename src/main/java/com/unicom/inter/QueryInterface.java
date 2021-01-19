package com.unicom.inter;

import com.unicom.entity.LoadColumnInfo;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public interface QueryInterface {
    /**
     为了验证数据的正确性，对数据进行插入HDFS
     */
    void put2Hdfs(String str, LoadColumnInfo loadColumnInfo, String path, String account ,FileSystem fs, FSDataOutputStream fileOutputStreamB, FSDataOutputStream fileOutputStreamC ) throws IOException;
}

