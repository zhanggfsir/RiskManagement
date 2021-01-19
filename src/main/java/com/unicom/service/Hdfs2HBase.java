package com.unicom.service;

import com.unicom.entity.*;
import com.unicom.impl.individualizationImpl.*;
import com.unicom.inter.ParseInterface;
import com.unicom.impl.*;
import com.unicom.tools.InsertTable;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;


/**
 * 读取hdfs文件，写入hbase
 */
public class Hdfs2HBase {
    private static Logger logger = LoggerFactory.getLogger(Hdfs2HBase.class);
    private static final Object lock = new Object();
    private int  batchCount=5000;
    private String implMethod="getPut";

    private Configuration configuration;

    private FileSystem fs;
    private ZkInfo zkInfo;
    CreateTableInfo createTableInfo;
    LoadColumnInfo loadColumnInfo;
    private Path path;
    private String tableName;
    private String account;
    private Class<?> implClass;
    private Object instance;
    AtomicLong atomicLong;


    public Hdfs2HBase(FileSystem fs, ZkInfo zkInfo, CreateTableInfo createTableInfo, LoadColumnInfo loadColumnInfo, Path path, String tableName,String account,Class<?> implClass,Object instance,AtomicLong atomicLong) {
        this.configuration = HBaseConfiguration.create();
        this.configuration.set("hbase.zookeeper.quorum", zkInfo.getZkQuorum());
        this.configuration.set("zookeeper.znode.parent", zkInfo.getZkParent());
        this.fs = fs;
        this.zkInfo = zkInfo;
        this.createTableInfo=createTableInfo;
        this.loadColumnInfo=loadColumnInfo;
        this.path = path;
        this.tableName=tableName;
        this.account=account;
        this.implClass=implClass;
        this.instance=instance;
        this.atomicLong=atomicLong;
    }

    public Hdfs2HBase(ZkInfo zkInfo, CreateTableInfo createTableInfo, LoadColumnInfo loadColumnInfo, String path, String tableName, String account) {
        this.configuration = HBaseConfiguration.create();
        this.configuration.set("hbase.zookeeper.quorum", zkInfo.getZkQuorum());
        this.configuration.set("zookeeper.znode.parent", zkInfo.getZkParent());
        this.zkInfo = zkInfo;
        this.createTableInfo=createTableInfo;
        this.loadColumnInfo=loadColumnInfo;
        this.path = new Path(path);
        this.tableName=tableName;
        this.account=account;
    }

    /**
     * 将InputStreamReader里的数据写入到hbase的table里面
     * @param fileReader 从文件里面读取的数据，如果压缩文件，这里是经过压缩的
     * @param table hbase的目标表
     * @throws Exception 读buff或写hbase异常
     */
    private void doPut(InputStreamReader fileReader, Table table) throws Exception{
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String str;
        List<Put> list = new ArrayList<>();
        long count = 0L;
        long sum = 0L;
        // 正式写入hbase
        while ((str = bufferedReader.readLine()) != null) {
            count += 1;
            Put put = null;

            //接收參數類型 & 參數
            put = (Put) implClass.getDeclaredMethod(implMethod, String.class,LoadColumnInfo.class, Path.class,String.class ).invoke(instance, str, loadColumnInfo, path, account);

            if (put != null) {
                list.add(put);
                sum += 1;
                atomicLong.getAndIncrement();
            }
            // 批量写入
            if (list.size() > this.batchCount) {
                try {
                    table.put(list);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("入库错误,丢弃数据 " + this.path.toString());
                    System.exit(1);
                }
                list.clear();
            }
        }
        // 清理资源
        try {
            table.put(list);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        table.close();
        list.clear();
        logger.info(this.path.toString() +",库:"+ this.zkInfo.getZkParent()+", 记录数: " + count + ", 入库记录数: " + sum+" totalLine:"+ atomicLong);

    }

    /**
     * 功能入口，实现读文件并入库
     *
     * @throws Exception e
     */
    public void run() throws Exception {
        InputStreamReader fileReader = null;
        DataInputStream inflateIn;

        logger.info("开始执行入库: " + this.path.toString());

        // 获得hbase连接池
        Connection connection = null;
        Table table = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            lock.notifyAll();
        }
        if (table == null) {
            System.exit(1);
        }

        // 处理压缩格式
        InputStream inputStream = this.fs.open(path, 8192);
        String[] split = path.getName().split("\\.");
        if (loadColumnInfo.getCompressType().equalsIgnoreCase("gz") && "gz".equals(split[split.length-1])) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            fileReader = new InputStreamReader(gzipInputStream);
            doPut(fileReader, table);
            gzipInputStream.close();
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("bzip2")) {
            BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
            fileReader = new InputStreamReader(bZip2CompressorInputStream);
            doPut(fileReader, table);
            bZip2CompressorInputStream.close();
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("txt")) {
            fileReader = new InputStreamReader(inputStream);
            doPut(fileReader, table);
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("snappy")) {
            int bufferSize = 262144;
            CompressionInputStream inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
            inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
            fileReader = new InputStreamReader(inflateIn);
            doPut(fileReader, table);
            inflateIn.close();
            inflateFilter.close();
        } else {
            logger.error("文件的压缩类型必须是txt、snappy、gz或bzip2! 当前文件压缩类型："+loadColumnInfo.getCompressType());
            System.exit(-1);
        }

        connection.close();
        fileReader.close();
        inputStream.close();

    }


    /**
     * 功能入口，实现读文件并入库
     *
     * @throws Exception e
     */
    public void runFromLocalPath() throws Exception {
        InputStreamReader fileReader = null;
        DataInputStream inflateIn;

        logger.info("开始执行入库: " + this.path.toString());

        // 获得hbase连接池
        Connection connection = null;
        Table table = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            lock.notifyAll();
        }
        if (table == null) {
            System.exit(1);
        }

        // 处理压缩格式
        FileInputStream inputStream = new FileInputStream(new File(path.toString()));

        if (loadColumnInfo.getCompressType().equalsIgnoreCase("gz")) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            fileReader = new InputStreamReader(gzipInputStream);
            doPut(fileReader, table);
            gzipInputStream.close();
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("bzip2")) {
            BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
            fileReader = new InputStreamReader(bZip2CompressorInputStream);
            doPut(fileReader, table);
            bZip2CompressorInputStream.close();
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("txt")) {
            fileReader = new InputStreamReader(inputStream);
            doPut(fileReader, table);
        } else if (loadColumnInfo.getCompressType().equalsIgnoreCase("snappy")) {
            int bufferSize = 262144;
            CompressionInputStream inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
            inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
            fileReader = new InputStreamReader(inflateIn);
            doPut(fileReader, table);
            inflateIn.close();
            inflateFilter.close();
        } else {
            logger.error("文件的压缩类型必须是txt、snappy、gz或bzip2! 当前文件压缩类型："+loadColumnInfo.getCompressType());
            System.exit(-1);
        }

        connection.close();
        fileReader.close();
        inputStream.close();

    }

}
