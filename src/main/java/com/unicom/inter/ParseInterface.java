package com.unicom.inter;

import com.unicom.entity.LoadColumnInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;

public interface ParseInterface {

    Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account);
}
