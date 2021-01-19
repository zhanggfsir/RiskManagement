package com.unicom.queryImpl.compare._20191110;

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

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.compare._20191110.UserDailyMsisdnTest20191124 419 user_daily_msisdn_20191124_test ui nouse 20191111

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


                "2EE83E6C06131CF82EFDFD724D19D8BE",
                "4FDB2EAE2C06E104BD999DA695FEFE0E",
                "ADC509EB348EB8CFA34BE202254D2F24",
                "82739875A34593BFBC194A3D243CAA2A",
                "56ECE40706BD107EE15E0BB93757E1CE",
                "669E6ADD9CAB1E631066BD0B0C6A4197",
                "6AC0368C9C7C78AF4348885B208A9EDB",
                "5384E34A9EBD537989DE5D5370855070",
                "C5BAD868FD4455005B47D6E0BB2AD663",
                "C2C37FC73189A9018B9F556DB2132162",
                "3D48047FAB4DEDFA8FE6A95DD1182431",
                "FD60743D54A4D5029D892091C11686A0",
                "B42F4F222B77B12527E7DDED2868BA03",
                "BC97A6C58F673536A359AB8E6526281A",
                "D5C0DB57FC7D367391865397ED2DF426",
                "7ECD184601E86A03EEEB1E87BD479E36",
                "3EE8B19A25987A142854FF5C391DFEBC"
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
