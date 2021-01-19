package com.unicom.impl.msisdnImpl;

import com.unicom.utils.HdfsUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
    将70E手机号 存到HDFS，写入Hive
 */
public class Msisdn2Hive {
    static Logger logger = LoggerFactory.getLogger(Msisdn2Hive.class);
    public static void main(String[] args) throws IOException {
        String xml319xl="319xml";
        long start=13000000000l;
        long end=19999999999l; //6000 0000
        long flag;
        FileSystem fs= HdfsUtil.getFs(xml319xl);
        String path="/user/ubd_test/ubd_risk_test.db/dxc/devicenumner";
        FSDataOutputStream save2HdfsfileOutputStream= HdfsUtil.getFileOutPutStream(fs,path);
        for(long i=start;i<end;i++){
            flag=i;
            if(flag%20000000 ==0){ //20000000
                logger.info("库:"+ "  /hbase_cx  "+", 记录数: " +flag + ", 入库记录数: " );
                flag=0;
            }
            HdfsUtil.writeLine2Hdfs(save2HdfsfileOutputStream,String.valueOf(i));
        }
    }
}
