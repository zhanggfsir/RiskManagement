package com.unicom.queryImpl.old;

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

//java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.queryImpl.old.QueryTableUI_conn_old3 419 user_check20191117 q nouse 20191117

public class QueryTableUI_conn_old3 {
    static Logger logger = LoggerFactory.getLogger(QueryTableUI_conn_old3.class);
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
                "C5583CDBE0EF7C0C7AB63F1787CC0E05",
                "BCDA4F322FAB21D4EAA00D39127AC687",
                "7C573A0729222AE25A493C23B8707718",
                "00897501727099BCB5CFFE52B973CD4B",
                "3F3C346BCC48D8C98AC4DB73A8C5C3EC",
                "7C13B015C28D30A053A47D7376487D63",
                "6AEEC30DE54E74D08ECEE654F0713CDE",
                "D3E7D3206E4C9DEA1875CEE9087CD97D",
                "30FEF184446139BCB8BC86BAD16E612F",
                "2D658724ABF5DFC9D11ABBC76BC3E535",
                "E23B8E6A09A1856884EEECA03BFC8B54",
                "B9CE3466F401A096C23B1BA7857A8136",
                "1A3F1A540CBA69083709CBCA67A932A9",
                "3C87E46B64651BA78BAEF39BC78DC407",
                "BF7C5C34978846B25D758B30222F70BB",
                "2FE2A4E82BA851821E277884EDB36A06",
                "530B5C8C0A54EAFC743F4915082D3AC0"
        };

        int i=0;
        for(String str: arr) {
            byte[] rowkey = Bytes.toBytes((short)(str.hashCode() & 0x7FFF));
            rowkey = Bytes.add(rowkey, Bytes.toBytes(str));
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
