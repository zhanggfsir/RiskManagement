package com.unicom.tools;

import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.service.GetConfigInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DropTable {
    private static Logger logger = LoggerFactory.getLogger(DropTable.class);
    private static final Object lock = new Object();

    public static void main(String[] args) {
        //删除表 参数2个 zkName tableName
        String zkName = null;
        String tableName = null;
        if (args.length == 2) {
            zkName=args[0];
            tableName = args[1];
        } else {
            logger.error("参数错误");
        }
        System.out.println("zkname ："+zkName +"  tableName : "+tableName);

        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo configZk =getConfigInfo.getZkInfo(zkName,tableName);
//        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);

        for (String parent : configZk.getZkParent().split(",")) {
            for (String table : tableName.split(","))
                delete(configZk, table);
        }

    }

    private static void delete(ZkInfo configZk, String tableName) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", configZk.getZkQuorum());
        configuration.set("zookeeper.znode.parent", configZk.getZkParent());

        // 获得hbase连接池
        Connection connection = null;
        Admin admin = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
            lock.notifyAll();
        }
        try {
            assert connection != null;
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            assert admin != null;
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));

                logger.info("表{}删除完成!", tableName);
            } else {
                logger.error("表{}不存在", tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (null != admin)
                try {
                    admin.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }


}
