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
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

public class CreateTable {
    private static Logger logger = LoggerFactory.getLogger(CreateTable.class);
    private static final Object lock = new Object();

    public static void main(String[] args)  {
        //建表 参数1个 zkName tableName
        String zkName = null;
        String tableName = null;
        if (args.length == 2) {
            zkName=args[0];
            tableName = args[1];
        } else {
            logger.error("参数错误");
        }

        //分别获取 configZk  createTableInfo
        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo configZk =getConfigInfo.getZkInfo(zkName,tableName);
        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);
        logger.info("---configZk--->"+configZk.toString());
        logger.info("---createTableInfo--->"+createTableInfo.toString());
        for (String parent : configZk.getZkParent().split(",")) {
            for (String table : tableName.split(","))
                create(configZk,createTableInfo, table);
        }
    }

    private static void create(ZkInfo configZk,CreateTableInfo createTableInfo, String tableName)  {


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
            logger.info("hbase库是:{}............", configZk.getZkParent());
            assert admin != null;
            if (admin.tableExists(TableName.valueOf(tableName))) {
                logger.error("{}表已经存在，无法创建！", tableName);
            } else {
                logger.info("正在创建表:{}..............", tableName);
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                //configTableInfo
                for (String familyName : createTableInfo.getFamilyName().split(",")) {
                    HColumnDescriptor hd = new HColumnDescriptor(familyName);
                    hd.setBlockCacheEnabled(createTableInfo.isBlockCacheEnabled());
                    hd.setBlocksize(createTableInfo.getBlockSize());
                    hd.setBloomFilterType(BloomType.valueOf(createTableInfo.getBloomFilterType()));
                    hd.setCacheBloomsOnWrite(createTableInfo.isCacheBloomsOnWrite());
                    hd.setCacheDataOnWrite(createTableInfo.isCacheDataOnWrite());
                    hd.setCacheIndexesOnWrite(createTableInfo.isCacheIndexesOnWrite());
                    hd .setCompressionType(Compression.Algorithm.valueOf(createTableInfo.getCompressionType().toUpperCase()));
                    hd.setDataBlockEncoding(DataBlockEncoding.valueOf(createTableInfo.getDataBlockEncoding()));
                    hd.setInMemory(createTableInfo.isInMemory());
                    hd.setMaxVersions(createTableInfo.getMaxVersions());
                    hd.setMinVersions(createTableInfo.getMinVersions());
                    hd.setTimeToLive(createTableInfo.getTimeToLive());
                    hd.setScope(createTableInfo.getScope());
//                    hd.setKeepDeletedCells("xxx");
//                    hd.setCompressTags(true);
                    hTableDescriptor.addFamily(hd);
                }

                admin.createTable(hTableDescriptor,
                        Bytes.toBytes((short) (0x7FFF / createTableInfo.getRegionNum())),
                        Bytes.toBytes((short) (0x7FFF - (0x7FFF / createTableInfo.getRegionNum()))),
                        createTableInfo.getRegionNum());
                logger.info("表创建完成:{}!", tableName);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (null != admin)
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.info("表连接关闭失败:{}!", tableName);
                    e.printStackTrace();
                }
        }
    }
}