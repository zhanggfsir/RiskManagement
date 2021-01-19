package com.unicom.utils;

import com.unicom.tools.QueryBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HdfsUtil {
    private static Logger logger = LoggerFactory.getLogger(HdfsUtil.class);
    /**
     * 写数据到HDFS
     * @param fsDataOutputStream
     * @param line
     */
    public static void writeLine2Hdfs(FSDataOutputStream fsDataOutputStream, String line) {
        try {
            fsDataOutputStream.writeBytes(line+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将HashMap 写出到HDFS
     * @param hiveOutputStream
     * @param hdfsMap
     */
    public static void saveHashMap2Hdfs(FSDataOutputStream hiveOutputStream, HashMap<String, String> hdfsMap) {
        // 所有数据最中写入文件
        Set<Map.Entry<String, String>> entrySet = hdfsMap.entrySet();
        for (Map.Entry<String, String> entry : entrySet) {
            String k = entry.getKey();
            String v = entry.getValue();
            //写入文件
            HdfsUtil.writeLine2Hdfs(hiveOutputStream, k +"|"+v);
        }
    }


    /**
     * 通过传入 fs path 获得写文件流，可以直接调用writeBytes方法将文件写到Hdfs
     * @param path
     * @return
     * @throws IOException
     */
    public static FSDataOutputStream getFileOutPutStream(FileSystem fs, String path) throws IOException {
        FSDataOutputStream fsfileOutputStream;
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            fsfileOutputStream = fs.create(hdfsPath,false);
        }else{
            fsfileOutputStream = fs.append(hdfsPath);
        }
        return fsfileOutputStream;
    }

    public static FileSystem getFs(String xml319) {

        Configuration hdfsConf = new Configuration();
        // HbaseTest_6  319xml
        InputStream hdfdSite= QueryBatch.class.getClassLoader().getClass().getResourceAsStream("/"+xml319+"/hdfs-site.xml");
        InputStream coreSite= QueryBatch.class.getClassLoader().getClass().getResourceAsStream("/"+xml319+"/core-site.xml");
        //InsertTable.class.getClassLoader().getResource()
        hdfsConf.addResource(hdfdSite);
        hdfsConf.addResource(coreSite);
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.setBoolean("dfs.support.append", true);
        hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hdfsConf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        FileSystem fs = null;
        //String nameService="hdfs://beh";
        //for (String hdfs : hdfsAddress) {
        try {
            //hdfsConf.set("fs.defaultFS", loadColumnInfo.getNameService());
            fs = FileSystem.get(hdfsConf);
            fs.getStatus();
            logger.info("hdfs连接成功!");
        } catch (IOException ex) {
            ex.printStackTrace();
            logger.error("hdfs连接失败,尝试连接下个namenode节点");
            System.exit(1);
        }
        if (fs == null) {
            logger.error("获取文件列表失败");
            System.exit(1);
        }
        return fs ;
    }

}
