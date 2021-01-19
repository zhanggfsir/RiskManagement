package com.unicom.utils;

import com.unicom.entity.ZkInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseUtil {

    public static Table getTable(ZkInfo zkInfo, String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",zkInfo.getZkQuorum()); // 319 quorum parent  table name
        configuration.set("zookeeper.znode.parent",zkInfo.getZkParent());

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        return table;
    }
}
