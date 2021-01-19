package com.unicom.testDemo.putAndGetFromHbase;

import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.testDemo.putAndGetFromHbase.ProtoPutHbaseAndGet

 */
public class ProtoPutHbaseAndGet {
    private static final Object lock = new Object();
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    public static void main(String[] args) throws IOException {
        String tableName="zhanggf_test";
        int batchCount=5000;
        String account="201909";
        //准备数据
        ArrayList<String> datalist=new ArrayList<>();
        datalist.add("lizongsheng|男|90|50.0");
        datalist.add("zhoujielun|男|91|32.0");
        datalist.add("Victoria|女|92|18.0");
        //获得连接对象，创建Table
        Configuration configuration = getHBaseConfiguration();
        Table table = null;

        table = getHbaseTable(tableName, datalist, table, configuration);

        List<Put> putList = new ArrayList<>();
        long count = 0L;
        long sum = 0L;
        // 正式写入hbase
        for (String data:datalist){
            count += 1;
            //拼接得到rowkey 一次读一行，得到put对象放入List
            Put put=getPut(data,account);

            if (put != null) {
                putList.add(put);
                sum += 1;
            }
            // 批量写入
            if (putList.size() > batchCount) {
                try {
                    //table.put(put); put同时支持一个put对象和一个put的list
                    table.put(putList);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("入库错误,丢弃数据 " + data);
                    System.exit(1);
                }
                putList.clear();
            }
        }

        //不足batch量的数据入库，清理资源
        try {
            table.put(putList);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }finally {
            table.close();
            configuration.clear();
        }

        putList.clear();
        logger.info(",库:/hbase_cx下 "+", 记录数: " + count + ", 入库记录数: " + sum);
    }

    private static Table getHbaseTable(String tableName, ArrayList<String> datalist, Table table, Configuration configuration) {
        synchronized (lock) {
            try {
                Connection connection = ConnectionFactory.createConnection(configuration);
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

        for (String str:datalist){
            System.out.println(str);
        }
        return table;
    }

    private static Configuration getHBaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "dsj-419-4t-56:2181,dsj-419-4t-57:2181,dsj-419-4t-58:2181");
        configuration.set("zookeeper.znode.parent", "/hbase_zx");
        return configuration;
    }

    private static Put getPut(String str,String account) {
        String familyName="f";
        String columnName="mc";
        String seperator="|";
        byte[] buff ;
        byte[] temp;

        String[] dataArray = StringUtils.splitPreserveAllTokens(str,seperator);

        Test.Actor.Builder actor=Test.Actor.newBuilder();
        actor.setSex(dataArray[1]);
        actor.setScore(Integer.parseInt(dataArray[2]));
        actor.setAge((int)Double.parseDouble(dataArray[3]));

        // 表: zhanggf_test
        // 列: f:mc
        // 数据样例 lizongsheng|男|90|50.0
        //buff=(dataArray[1]+dataArray[2]).getBytes(); // 之前样式
        buff=actor.build().toByteArray();
        //得到rowkey
        StringBuilder keyValue=new StringBuilder();
        String name=dataArray[0].toUpperCase();
        keyValue.append(name);
        keyValue.append(account);
        //手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (name.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(familyName), Bytes.toBytes(columnName), buff);
        return put;
    }
}
