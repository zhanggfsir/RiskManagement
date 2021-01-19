package com.unicom.impl.msisdnImpl;


import com.unicom.risk.Risk;
import com.unicom.utils.Encryption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/*
通过手机号 sha256 反查 md5
运行方式
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.impl.msisdnImpl.Sha256Md5 319 >319md5.log &
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.impl.msisdnImpl.Sha256Md5 419 >419md5.log &

dim_msisdn_sha256_md5
 */
class Sha256Md5 {
    static Logger logger = LoggerFactory.getLogger(Sha256Md5.class);
    private static final Object lock = new Object();
    static final int  BatchCount=5000;
    private static final String family="f";
    private static final String qualifier="md5";
    private static final String tableName="dim_msisdn_sha256_md5";



    public static void main(String[] args) throws IOException, InterruptedException {

        int threadNum=7;
        long start=13000000000l;
        long end=19999999999l; //6000 0000
        long perSize=(end-start+1)/threadNum;
        //得到Hbase表
        Configuration configuration = getConfiguration(args[0]);
        Table tableName=getTableName(configuration);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        CountDownLatch countDownLatch=new CountDownLatch(threadNum);
        AtomicLong ai=new AtomicLong(0);
//        int perSize=dataList.size()/threadNum;
//        System.out.println("--------单个线程处理数据量 ----------"+perSize);
        for(int i=0;i<threadNum;i++) {
            WorkTask task=new WorkTask();
            task.setTableName(tableName);
            task.setQualifier(qualifier);
            task.setCountDownLatch(countDownLatch);
            task.setAi(ai);
            task.setPerSize(perSize);
            task.setStart((start+i*perSize));
            executorService.submit(task);
        }
        countDownLatch.await();
        executorService.shutdown();

    }

    private static Table getTableName(Configuration configuration) throws IOException {
        // 获得hbase连接池
        Connection connection = null;
        Table table = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                table = connection.getTable(TableName.valueOf(tableName));
                return table;
            } catch (Exception e) {
                connection.close();
                e.printStackTrace();
                }
            }
        if (table == null) {
            connection.close();
            System.exit(1);
        }
        connection.close();
        return null;
    }

    private static Configuration getConfiguration(String zkName) {
        Configuration configuration = HBaseConfiguration.create();
        switch (zkName){
            case "319":
                configuration.set("hbase.zookeeper.quorum", "");
                configuration.set("zookeeper.znode.parent", "/hbase_cx");;
                break;
            case "419":
                configuration.set("hbase.zookeeper.quorum", "");
                configuration.set("zookeeper.znode.parent", "/hbase_zx");
                break;
            default:
                System.out.println("输入参数不合法");
        }

        return configuration;
    }

    public static Put getPut(long data) {
        Risk.Sha256Md5.Builder sha256Md5=Risk.Sha256Md5.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String deviceNumberMd5=Encryption.md5(String.valueOf(data));
        sha256Md5.setDeviceNumberMd5(deviceNumberMd5);

        String deviceNumber256=Encryption.sha256(String.valueOf(data));

        buff = sha256Md5.build().toByteArray();

        // rowkey: device_number_sha256
        temp = Bytes.toBytes((short) (deviceNumber256.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(deviceNumber256));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(family), Bytes.toBytes(qualifier), buff);
        return put;
    }

}
