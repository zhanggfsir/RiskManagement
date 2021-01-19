package com.unicom.queryImpl.compare._20191109;

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

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.compare._20191111.UserDailyMsisdnTest20191124_4 419 user_daily_msisdn_20191124_test ui nouse 20191111

public class UserDailyMsisdnTest20191124_4 {
    static Logger logger = LoggerFactory.getLogger(UserDailyMsisdnTest20191124_4.class);
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


                "F437AC84B5A0B4C54974AC91099C1264",
                "06C60E350CB53222373E00D3FFF0C883",
                "D164793A41FB4F1C8DB3A22BB1B20464",
                "2CD28D10051D83CBBB710E56C82EC92C",
                "05E098459D18338C688A23B2AE3D7464",
                "9E097ED9DD9892E98676CDD0729A0CF4",
                "7DD7CECDBD96D66AE2FB06AFA2F99B33",
                "5DDD94BF7E72E8A9465AD659A80D62B9",
                "4464C3B1A32A050DD17B61CEF4BB360A",
                "210B2D32E7F944AEAF0529365E810501",
                "4909BD49B9DE887AEBC7F5B04A8804EF",
                "D89EF9D94A668C7A5515F2BF4B3BD82E",
                "5ADF31B419EB51ABA0BC737AC52E12B5",
                "13C305139B393C4D04C3E84FF7503FCE",
                "3E61072C4F285C04B82253AC4D6281D7",
                "54BE3264CAD0D256F88AC9C4D8C1DEB0",
                "7CE0A9C3B3C2D402F2225686DF32192C"
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
