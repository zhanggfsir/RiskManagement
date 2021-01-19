package com.unicom.tools;

import java.io.IOException;

import com.unicom.entity.ZkInfo;
import com.unicom.service.GetConfigInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.MajorCompactTable 419 slowquery_test

public class MajorCompactTable {
    static Logger logger = LoggerFactory.getLogger(MajorCompactTable.class);
    private static final Object lock = new Object();

    public static void main(String[] args) {
        String zkName = null;
        String tableName = null;
        if (args.length == 2) {
            zkName = args[0];
            tableName = args[1];
        } else {
            logger.error("参数错误");
        }

        GetConfigInfo getConfigInfo = new GetConfigInfo();
        ZkInfo configZk = getConfigInfo.getZkInfo(zkName,tableName);

        for (String parent : configZk.getZkParent().split(",")) {
            majorCompactTable(configZk, tableName);

        }
    }
        private static void majorCompactTable(ZkInfo configZk, String tableName) {

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
                    admin.majorCompact(TableName.valueOf(tableName));

                    logger.info(" Compact 表{} 完成!", tableName);
                } else {
                    logger.error("表{}不存在", tableName);
                }
            } catch (IOException e) {
                logger.error(" Compact 表{}出现错误!", tableName);
                e.printStackTrace();
            } finally {
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

