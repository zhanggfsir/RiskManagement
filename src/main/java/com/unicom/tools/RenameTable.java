package com.unicom.tools;

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

import java.io.IOException;

public class RenameTable {
    private static Logger logger = LoggerFactory.getLogger(DropTable.class);
    private static final Object lock = new Object();

    public static void main(String[] args) {
        //删除表 参数2个 zkName tableName
        String zkName = null;
        String oldTableName = null;
        String newTableName=null;
        if (args.length == 3) {
            zkName=args[0];
            oldTableName = args[1];
            newTableName = args[2];
        } else {
            logger.error("参数错误");
        }
        System.out.println("zkname ："+zkName +"  old : "+oldTableName+" new : "+ newTableName);

        //分别获取 configZk  createTableInfo
        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo configZk =getConfigInfo.getZkInfo(zkName,oldTableName);

        for (String parent : configZk.getZkParent().split(",")) {
                rename(configZk, oldTableName,newTableName);
        }

    }

    private static void rename(ZkInfo configZk, String oldTableName,String newTableName) {
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
            if (admin.tableExists(TableName.valueOf(oldTableName))) {

                String snapshotName = "tmp_"+oldTableName;
                admin.disableTable(TableName.valueOf(oldTableName));
                admin.snapshot(snapshotName, TableName.valueOf(oldTableName));
                admin.cloneSnapshot(snapshotName, TableName.valueOf(newTableName));
                admin.deleteSnapshot(snapshotName);
                admin.deleteTable(TableName.valueOf(oldTableName));

                logger.info("表{}删除完成!", oldTableName);
            } else {
                logger.error("表{}不存在", oldTableName);
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
