package com.unicom.queryImpl;

import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.risk.Risk;
import com.unicom.service.GetConfigInfo;
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
import java.util.List;

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.QueryTablePt 319 dim_msisdn_sha256_md5 pt 262CA5841FCF3D2936646608F90AEE61

public class QueryTablePt {
    static Logger logger = LoggerFactory.getLogger(QueryTablePt.class);

    public static void main(String[] args) {

        String zkName = null;
        String tableName = null;
        String qualifier = null;
        String deviceNumberEn = null;
        String account = null;

        if (args.length==4){
            zkName=args[0];
            tableName=args[1];
            qualifier=args[2];
            deviceNumberEn=args[3].toUpperCase();
            account="";
        } else if (args.length==5){
             zkName=args[0];
             tableName=args[1];
             qualifier=args[2];
            deviceNumberEn=args[3].toUpperCase();
            account=args[4];

        }else if(args.length==6){
            zkName=args[0];
            tableName=args[1];
            qualifier=args[2];
            deviceNumberEn=args[3].toUpperCase();
            account=args[4]+args[5];
        }else{
            logger.error("参数不合法");
            System.exit(-1);
        }

        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);

        if ((null != deviceNumberEn) && (!deviceNumberEn.isEmpty())) {
            String[] rowkeyAll = deviceNumberEn.split(",");
            byte[] hash = Bytes.toBytes((short)(rowkeyAll[0].hashCode() & 0x7FFF));
            byte[] rowkey = hash;

            for (String oneKey : rowkeyAll) {
                rowkey = Bytes.add(rowkey, Bytes.toBytes(oneKey+account));
            }

            get(zkInfo, rowkey,tableName,createTableInfo.getFamilyName(),qualifier);
        } else {
            logger.info("rowkey未指定");
        }

    }

    private static void get(ZkInfo zkInfo, byte[] row,String tableName,String familyName,String qualifier) {
        logger.info("正在查询指定的rowkey........");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkInfo.getZkQuorum());
        configuration.set("zookeeper.znode.parent", zkInfo.getZkParent());
        Connection connection = null;
        Table table=null;

        try
        {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(row);
            get.setCacheBlocks(zkInfo.isCache());
            get.setMaxVersions(zkInfo.getMaxVersion());
            logger.debug("get查询的最大版本数是{},注意查看结果是否出错", zkInfo.getMaxVersion());

            long  start=System.currentTimeMillis();
            Result result = table.get(get);
            long  end=System.currentTimeMillis();
            System.out.println("耗时为 ---> "+(end-start)+" ");

            if (!result.isEmpty())
            {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    StringBuilder sb = new StringBuilder();
                    i = 1;
                    for (Cell cell : list)
                    {
                        Risk.Md5Plaintext md5Plaintext = Risk.Md5Plaintext.parseFrom(CellUtil.cloneValue(cell));
                        sb.append(md5Plaintext.getDeviceNumber());sb.append("|");
                        sb.append(md5Plaintext.getServType());sb.append("|");
                        sb.append(md5Plaintext.getProvId());sb.append("|");
                        sb.append(md5Plaintext.getAreaId());sb.append("|");
                        sb.append(md5Plaintext.getTurnOutDealer());sb.append("|");
                        sb.append(md5Plaintext.getEffectDate());
                        logger.info(sb.toString());
                        logger.info("-------------------------------------------");

                        String str = Bytes.toString(CellUtil.cloneValue(cell));
                        logger.info("表{}在列族{}列{}的查询版本{}结果是:{}", new Object[] { tableName, family, qualifier, i, str });
                        i++;
                    }
                }
            } else {
                logger.info("未查询到结果!");
            }

            if (null != table)
                try {
                    table.close();
                    connection.close();
                } catch (IOException e) {
                    logger.error("关闭表对象出错");
                    e.printStackTrace();
                }
        }
        catch (IOException e)
        {
            logger.error("查询单条记录出错");
            e.printStackTrace();

            if (null != table)
                try {
                    table.close();
                    connection.close();
                } catch (IOException ioe) {
                    logger.error("关闭表对象出错");
                    ioe.printStackTrace();
                }
        } finally
        {
            if (null != table)
                try {
                    table.close();
                    connection.close();
                } catch (IOException e) {
                    logger.error("关闭表对象出错");
                    e.printStackTrace();
                }
        }
    }
}
