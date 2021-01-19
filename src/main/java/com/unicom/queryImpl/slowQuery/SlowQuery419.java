package com.unicom.queryImpl.slowQuery;

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

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.slowQuery.SlowQuery419.QueryTableUI_conn 419 user_daily_msisdn ui nouse 20191117

public class SlowQuery419 {
    static Logger logger = LoggerFactory.getLogger(SlowQuery419.class);
    public static void main(String[] args) throws IOException {

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
        Configuration configuration =getConfiguration();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

//        GetConfigInfo getConfigInfo=new GetConfigInfo();
//        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
//        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);
        String arr[]={
                "E6075B78F6762BFA857830F73FF8082A",
                "4D9AFFBF9E34D9EF025A75C7415F098F",
                "480C4E80FAEDF639A8C07E7178D1D4FC",
                "7C0DF2127F2D06B7525271C2164CE049",
                "9D1926083A64F92C3E3EA359C307D7D7",
                "D97B4E2CECE8EA21F9FADACE49A8A073",
                "D1B7FD16B624E9BDA02DAA8603C85A55",
                "E6CF09D5F20F4F63395DE0D4D55BE107",
                "894E9A2184CA34364949A59771E06A7C",
                "E27D42B1168CFF2DB15B2FA8466A93A2",
                "2F0FAFC0EA132FBAC02D25691F10D433",
                "BAE674376B83A0827021C67C8633FE80",
                "7B66E74FC961EF1CAD578B048495E788",
                "F9F3FF772CE49075C508FD073635BBB5"
        };

        int i=0;
        for(String str: arr) {
            byte[] rowkey = Bytes.toBytes((short)(str.hashCode() & 0x7FFF));
            rowkey = Bytes.add(rowkey, Bytes.toBytes(str+account));
            get(rowkey,table,"f",qualifier, i);
            i++;

        }

        close( configuration, connection, table);

    }

    private static Configuration getConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "dsj-419-4t-56:2181,dsj-419-4t-57:2181,dsj-419-4t-58:2181");
        configuration.set("zookeeper.znode.parent", "/hbase_zx");
        return configuration;
    }

    private static void get(byte[] row,Table table,String familyName,String qualifier,int i) {

        try
        {
            Get get = new Get(row);
            get.setCacheBlocks(true);
            get.setMaxVersions(1);
            long  start=System.currentTimeMillis();
            Result result = table.get(get);
            long  end=System.currentTimeMillis();
            System.out.println("------"+i+"------耗时为 ---> "+(end-start)+" ");

            if (!result.isEmpty())
            {
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    StringBuilder sb = new StringBuilder();
                    i = 1;
                    for (Cell cell : list)
                    {
//                        Risk.AlCustInfo alCustInfo = Risk.AlCustInfo.parseFrom(CellUtil.cloneValue(cell));
//                        sb.append(alCustInfo.getCertType());sb.append("|");
//                        sb.append(alCustInfo.getCustSex());sb.append("|");
//                        sb.append(alCustInfo.getCertAge());sb.append("|");
//                        sb.append(alCustInfo.getConstellationDesc());sb.append("|");
//                        sb.append(alCustInfo.getCustBirthday());sb.append("|");
//                        sb.append(alCustInfo.getCustNameMd5());sb.append("|");
//                        sb.append(alCustInfo.getNameMosaic());sb.append("|");
//                        sb.append(alCustInfo.getCertUsernums());sb.append("|");
//                        sb.append(alCustInfo.getCertInnetUsernums());sb.append("|");
//                        sb.append(alCustInfo.getCertBreakUsernums());sb.append("|");
//                        logger.info(sb.toString());

                        String str = Bytes.toString(CellUtil.cloneValue(cell));
                        logger.info("表{}在列族{}列{}的查询版本{}结果是:{}", new Object[] { table.toString(), family, qualifier, i, str });
                        i++;
                    }
                }
            } else {
                logger.info("未查询到结果!");
            }

        }catch (IOException e) {
            logger.error("============================");
        }

    }
//    get(byte[] row,String tableName,String familyName,String qualifier,int i) {
        private static void  close (Configuration configuration,Connection connection,Table table){

        if (null != table)
                try {
                    table.close();
                    connection.close();
                    configuration.clear();
                } catch (IOException e) {
                    logger.error("关闭表对象出错");
                    e.printStackTrace();
                }
                finally
                {
                        try {
                            table.close();
                            connection.close();
                        } catch (IOException e) {
                            logger.error("关闭表对象出错");
                            e.printStackTrace();
                        }
                }
    }


    public static class QueryTableUI_conn {
        static Logger logger = LoggerFactory.getLogger(QueryTableUI_conn.class);
        public static void main(String[] args) throws IOException {

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
            Configuration configuration =getConfiguration();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

    //        GetConfigInfo getConfigInfo=new GetConfigInfo();
    //        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
    //        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);
            String arr[]={
                    "E6075B78F6762BFA857830F73FF8082A",
                    "4D9AFFBF9E34D9EF025A75C7415F098F",
                    "480C4E80FAEDF639A8C07E7178D1D4FC",
                    "7C0DF2127F2D06B7525271C2164CE049",
                    "9D1926083A64F92C3E3EA359C307D7D7",
                    "D97B4E2CECE8EA21F9FADACE49A8A073",
                    "D1B7FD16B624E9BDA02DAA8603C85A55",
                    "E6CF09D5F20F4F63395DE0D4D55BE107",
                    "894E9A2184CA34364949A59771E06A7C",
                    "E27D42B1168CFF2DB15B2FA8466A93A2",
                    "2F0FAFC0EA132FBAC02D25691F10D433",
                    "BAE674376B83A0827021C67C8633FE80",
                    "7B66E74FC961EF1CAD578B048495E788",
                    "F9F3FF772CE49075C508FD073635BBB5"
            };

            int i=0;
            for(String str: arr) {
                byte[] rowkey = Bytes.toBytes((short)(str.hashCode() & 0x7FFF));
                rowkey = Bytes.add(rowkey, Bytes.toBytes(str+account));
                get(rowkey,table,"f",qualifier, i);
                i++;

            }

            close( configuration, connection, table);

        }

        private static Configuration getConfiguration() {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "dsj-419-4t-56:2181,dsj-419-4t-57:2181,dsj-419-4t-58:2181");
            configuration.set("zookeeper.znode.parent", "/hbase_zx");
            return configuration;
        }

        private static void get(byte[] row,Table table,String familyName,String qualifier,int i) {

            try
            {
                Get get = new Get(row);
                get.setCacheBlocks(true);
                get.setMaxVersions(1);
                long  start=System.currentTimeMillis();
                Result result = table.get(get);
                long  end=System.currentTimeMillis();
                System.out.println("------"+i+"------耗时为 ---> "+(end-start)+" ");

                if (!result.isEmpty())
                {
                    for (String family : familyName.split(",")) {
                        List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                        StringBuilder sb = new StringBuilder();
                        i = 1;
                        for (Cell cell : list)
                        {
    //                        Risk.AlCustInfo alCustInfo = Risk.AlCustInfo.parseFrom(CellUtil.cloneValue(cell));
    //                        sb.append(alCustInfo.getCertType());sb.append("|");
    //                        sb.append(alCustInfo.getCustSex());sb.append("|");
    //                        sb.append(alCustInfo.getCertAge());sb.append("|");
    //                        sb.append(alCustInfo.getConstellationDesc());sb.append("|");
    //                        sb.append(alCustInfo.getCustBirthday());sb.append("|");
    //                        sb.append(alCustInfo.getCustNameMd5());sb.append("|");
    //                        sb.append(alCustInfo.getNameMosaic());sb.append("|");
    //                        sb.append(alCustInfo.getCertUsernums());sb.append("|");
    //                        sb.append(alCustInfo.getCertInnetUsernums());sb.append("|");
    //                        sb.append(alCustInfo.getCertBreakUsernums());sb.append("|");
    //                        logger.info(sb.toString());

                            String str = Bytes.toString(CellUtil.cloneValue(cell));
                            logger.info("表{}在列族{}列{}的查询版本{}结果是:{}", new Object[] { table.toString(), family, qualifier, i, str });
                            i++;
                        }
                    }
                } else {
                    logger.info("未查询到结果!");
                }

            }catch (IOException e) {
                logger.error("============================");
            }

        }
    //    get(byte[] row,String tableName,String familyName,String qualifier,int i) {
            private static void  close (Configuration configuration,Connection connection,Table table){

            if (null != table)
                    try {
                        table.close();
                        connection.close();
                        configuration.clear();
                    } catch (IOException e) {
                        logger.error("关闭表对象出错");
                        e.printStackTrace();
                    }
                    finally
                    {
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
}
