package com.unicom.queryImpl.compare._20191117;

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

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.compare._20191117.UserDailyMsisdnTest20191124 419 user_daily_msisdn_20191124_test ui nouse 20191117

public class UserDailyMsisdnTest20191124 {
    static Logger logger = LoggerFactory.getLogger(UserDailyMsisdnTest20191124.class);
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

                "AB0D40504FC1355F59F3DB8F5DF9F555",
                "1C57F81AEEDF4F77EC18A37D8B473134",
                "B8BEF1690682998F993FF61806A5113F",
                "0A7B5CB7594EED8C457F0D7C439D00B9",
                "1DED55EB4B13AD2E1704C5F867B9EFC1",
                "89BED8E9C137469514E0DC51412AA38C",
                "A0A308B344974678A1EE39D94F14AA87",
                "96CE6711EDB8FA49901647F6B9120A9C",
                "E897AE30887D1439A142696625B25933",
                "1552F60562E58F63A8DDC747DAB1B3E9",
                "F518923D9B57F19F7DB50078C3A64C6C",
                "E63468C35575ED135801FCD2DEEDA6C9",
                "1BF9F3FFF88894EF0B27393866E47944",
                "794AAC2A83E669FC2700F00F0F016807",
                "48251A93A2E45AB2DF97778E38204DB0",
                "F3CBA02765EA7595232495608760BCE9",
                "356F5D3B5A6E1EB50DFD9D0ACB43EDBB"
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
