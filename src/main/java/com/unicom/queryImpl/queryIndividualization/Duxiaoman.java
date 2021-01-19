package com.unicom.queryImpl.queryIndividualization;

import com.unicom.risk.Individualization;
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

/**
 java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.queryIndividualization.Duxiaoman 419 p_duxiaoman_monthly
 q 262CA5841FCF3D2936646608F90AEE61 201909
*/

public class Duxiaoman {
    static Logger logger = LoggerFactory.getLogger(Duxiaoman.class);
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
//        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
//        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);

        if ((null != deviceNumberEn) && (!deviceNumberEn.isEmpty())) {
            String[] rowkeyAll = deviceNumberEn.split(",");
            byte[] hash = Bytes.toBytes((short)(rowkeyAll[0].hashCode() & 0x7FFF));
            byte[] rowkey = hash;
            for (String oneKey : rowkeyAll) {
                rowkey = Bytes.add(rowkey, Bytes.toBytes(oneKey+account));
            }

            get("319", rowkey,tableName,"f",qualifier);
        } else {
            logger.info("rowkey未指定");
        }

    }

    private static void get(String _319, byte[] row,String tableName,String familyName,String qualifier) {
        logger.info("正在查询指定的rowkey........");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "dsj-419-4t-56:2181,dsj-419-4t-57:2181,dsj-419-4t-58:2181");
        configuration.set("zookeeper.znode.parent", "/hbase_zx");
        Connection connection = null;
        Table table=null;

        try
        {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(row);
            get.setCacheBlocks(true);
            get.setMaxVersions(1);
            long  start=System.currentTimeMillis();
            Result result = table.get(get);
            long  end=System.currentTimeMillis();
            System.out.println("耗时为 ---> "+(end-start)+" ");
            System.out.println();

            if (!result.isEmpty())
            {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    StringBuilder sb = new StringBuilder();
                    i = 1;
                    for (Cell cell : list)
                    {
                        Individualization.PDuxiaomanMonthly duxiaomanMonthly = Individualization.PDuxiaomanMonthly.parseFrom(CellUtil.cloneValue(cell));
                        sb.append(duxiaomanMonthly.getUserId());sb.append("|");
                        sb.append(duxiaomanMonthly.getMonthId());sb.append("|");
                        sb.append(duxiaomanMonthly.getPredScoreA());sb.append("|");
                        sb.append(duxiaomanMonthly.getProvId());sb.append("|");

                        logger.info(sb.toString());

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

