package com.unicom.impl.msisdnImpl;


import com.unicom.utils.Encryption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Md5DimMsisdnSha256Md5 {
    private static Logger logger = LoggerFactory.getLogger(Md5DimMsisdnSha256Md5.class);
    private static final Object lock = new Object();
    private static final int  BatchCount=5000;
    private static final String family="f";
    private static final String qualifier="md5";

    public static void main(String[] args) {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "");
        configuration.set("zookeeper.znode.parent", "/hbase_cx");
        String tableName="dim_msisdn_sha256_md5";

        String[]  hdfsAddress={"hdfs://beh/"};

        ExecutorService executorService = Executors.newFixedThreadPool(4);
                executorService.execute(() -> {
                    try {
                        run(configuration,tableName);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                });

        executorService.shutdown();
        while (true) {
            try {
                if (executorService.isTerminated()) {
                    logger.info("所有的子线程都结束了");
                    break;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

    }
    public  static void run(Configuration configuration,String tableName) throws Exception {

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

        doPut(table);

        connection.close();

    }

    private static void doPut(Table table) throws Exception{
        List<Put> list = new ArrayList<>();
        BigInteger count=new BigInteger("0");;
        BigInteger sum=new BigInteger("0");;
        int flag=0;

        Put put=null;
        BigInteger data=new BigInteger("13000000000");;
        BigInteger end=new BigInteger("19999999999");;

        while (end.compareTo(data)>=0){
            flag++;
            count.add(new BigInteger("1"));
            data=data.add(new BigInteger("1"));
            put= getPut(data);


        if (put != null) {
            list.add(put);
            sum=sum.add(new BigInteger("1"));
        }
            // 批量写入
            if (list.size() > BatchCount) {
                try {
                    table.put(list);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("入库错误,丢弃数据 " );
                    System.exit(1);
                }
                list.clear();
            }

            if(flag%20000000 ==0){
                logger.info("库:"+ "  /hbase_cx  "+", 记录数: " +flag + ", 入库记录数: " + sum.toString());
                flag=0;
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
        logger.info("---------------------------入库完成---------------------------");
        logger.info("库:"+ "  /hbase_cx  "+", 记录数: " + count.toString() + ", 入库记录数: " + sum.toString());
    }

    public static Put getPut(BigInteger data) {
        byte[] temp = new byte[0];

        String deviceNumber256= Encryption.sha256(data.toString());
        String deviceNumberMd5= Encryption.md5(data.toString());

        StringBuilder keyValue=new StringBuilder();

        // rowkey: device_number_sha256
        temp = Bytes.toBytes((short) (deviceNumber256.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(deviceNumber256));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(deviceNumberMd5));
        return put;
    }

}
