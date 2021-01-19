package com.unicom.testDemo.putAndGetFromHbase;

import com.unicom.risk.Risk;
import com.unicom.utils.HdfsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.testDemo.putAndGetFromHbase.ProtoGetFromHbase

 */
public class ProtoGetFromHbase {
    private static final Object lock = new Object();
    private static Logger logger = LoggerFactory.getLogger(ProtoGetFromHbase.class);

    public static void main(String[] args) throws IOException {
        String tableName="zhanggf_test";
        String account="201909";
        String familyName="f";
        String columnName="mc";
        String seperator="|";
//        String readPathTmp = "/user/ubd_test/ubd_risk_test.db/dxc/tmp";
//        fileOutputStreamB= HdfsUtil.getFileOutPutStream(fs,saveHdfsPath+tableName+account+"319");

        //准备数据
        ArrayList<String> datalist=new ArrayList<>();
        datalist.add("lizongsheng|50.0|男");
        datalist.add("zhoujielun|32|男");
        datalist.add("Victoria|18|女");
        //获得连接对象，创建Table
        Configuration configuration = getHBaseConfiguration();
        Table table = null;

        table = getHbaseTable(tableName, table, configuration);

        List<Get> getList=new ArrayList<>();
        //TODO 传入的账期可能是6位的 此处 换成8位的 ！ 注意账期的长度
        for (String line:datalist){

            String[] dataArray = StringUtils.splitPreserveAllTokens(line, seperator);

            //获得主键
            String nameKey = dataArray[0].toUpperCase();
            byte[] hash = Bytes.toBytes((short) (nameKey.hashCode() & 0x7FFF));
            byte[] rowkey = Bytes.add(hash, Bytes.toBytes(nameKey + account));

            Get get = new Get(rowkey);
            // 当前默认值是false
            get.setCacheBlocks(false);
            get.setMaxVersions(1);
            getList.add(get);
        }
        getDataFromHbaseAndWrite(familyName, columnName, table, getList);
        table.close();

    }

    private static void getDataFromHbaseAndWrite(String familyName, String columnName, Table table, List<Get> getList) throws IOException {
        Result[] resultArray = table.get(getList);

        for (Result result : resultArray) {
            if (!result.isEmpty()) {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(columnName));
                    for (Cell cell : list) {
                        StringBuilder sb = new StringBuilder();
                        // 方法1 同之前
                        String str = Bytes.toString(CellUtil.cloneValue(cell));
                        logger.info("-----str------" + str);

                        // 方法2
                        Test.Actor actor = Test.Actor.parseFrom(CellUtil.cloneValue(cell));

                        sb.append(actor.getSex());
                        sb.append("|");
                        sb.append(actor.getScore());
                        sb.append("|");
                        sb.append(actor.getAge());
                        logger.info("-----sb------" + sb.toString());
                    }
                }
            } else {
                logger.info("未查询到结果!");
            }
        }
    }

    private static Table getHbaseTable(String tableName, Table table, Configuration configuration) {
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
        Risk.AlUserInfo.Builder alUserInfoBuild=Risk.AlUserInfo.newBuilder();
        byte[] buff ;
        byte[] temp;

        String[] dataArray = StringUtils.splitPreserveAllTokens(str,seperator);
        //alUserInfoBuild.setIsStat(4);
        //buff = alUserInfoBuild.build().toByteArray();
        //得到value     datalist.add("lizongsheng|50.0|男");
        buff=(dataArray[1]+dataArray[2]).getBytes();
        //得到rowkey
        StringBuilder keyValue=new StringBuilder();
        String name=dataArray[1].toUpperCase();
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
