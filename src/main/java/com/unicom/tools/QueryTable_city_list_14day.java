package com.unicom.tools;

import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.risk.Individualization;
import com.unicom.risk.Risk;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.Encryption;
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

public class QueryTable_city_list_14day {
    static Logger logger = LoggerFactory.getLogger(QueryTable_city_list_14day.class);

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

        // 对手机号进行处理 目标：对传入的明文或者md5值同时支持
        // TODO md5
        if(deviceNumberEn.length()==11){
            Encryption encryption=new Encryption();
            deviceNumberEn=encryption.md5(deviceNumberEn) ;
        }else if(deviceNumberEn.length()==32){
            deviceNumberEn=deviceNumberEn.toUpperCase();
        }else{
            System.out.println("unsupport device_number,please check the length of device_number");
        }


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
            Result result = table.get(get);

            if (!result.isEmpty())
            {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    StringBuilder sb = new StringBuilder();
                    i = 1;
                    for (Cell cell : list)
                    {
                        Individualization.CityList14Day cityList14Day = Individualization.CityList14Day.parseFrom(CellUtil.cloneValue(cell));
                        cityList14Day.getCityListOrBuilderList();
                        List<Individualization.CityList> cityList=cityList14Day.getCityListList();
                        // 先处理第一条
                        Individualization.CityList city=cityList.get(0);
                        sb.append("");sb.append(city.getProv()); sb.append("_");
                        sb.append("");sb.append(city.getArea());sb.append("_");
                        sb.append("");sb.append(city.getCity()); // sb.append("_");
                        // 如果有多条的处理
                        if (cityList.size()>1){
                           int len=cityList.size();
                           for (int j=1;j<=len-1;j++){
                             sb.append("#");
                               Individualization.CityList cityJ=cityList.get(j);
                               sb.append("");sb.append(cityJ.getProv()); sb.append("_");
                               sb.append("");sb.append(cityJ.getArea());sb.append("_");
                               sb.append("");sb.append(cityJ.getCity());// sb.append("_");
                           }
                         }

//                        for(Individualization.CityList city:cityList){
//                            sb.append("");sb.append(city.getProv()); sb.append("_");
//                            sb.append("");sb.append(city.getArea());sb.append("_");
//                            sb.append("");sb.append(city.getCity());sb.append("_");
//                        }
//                        System.out.println(sb.toString());

                        cityList14Day.getCityListOrBuilderList();
                        System.out.println("--------------------------------------------------------------------------------");
                        System.out.println("result:"+sb.toString());
//                        logger.info(sb.toString());

//                        userNmPermanent.getProvId();
//                        Risk.Point top1WorkPoint=userNmPermanent.getTop1Home();
//                        top1WorkPoint.getLatitude();
//                        String city=top1WorkPoint.getCity();
//
//                        userNmPermanent.getTop1Work();
//
//                        sb.append(alRnsWide.getIsGroup());
//                        logger.info(sb.toString());

                        /*
                        System.out.println("----------------------------------------");
                        Individualization.PYiQing yingQingImpl = Individualization.PYiQing.parseFrom(CellUtil.cloneValue(cell));
                        sb.append(yingQingImpl.getDateDtBytes());sb.append("|");
                        sb.append(yingQingImpl.getDeviceNumberMd5());sb.append("|");
                        sb.append(yingQingImpl.getProvName());sb.append("|");
                        sb.append(yingQingImpl.getCityName());sb.append("|");
                        sb.append(yingQingImpl.getCountyName());sb.append("|");
                        sb.append(yingQingImpl.getProvCode());sb.append("|");
                        sb.append(yingQingImpl.getCityCode());sb.append("|");
                        sb.append(yingQingImpl.getCountyCode());sb.append("|");
                        sb.append(yingQingImpl.getFromProv());sb.append("|");
                        sb.append(yingQingImpl.getFromCity());sb.append("|");
                        sb.append(yingQingImpl.getAwayDt());sb.append("|");
                        String str = Bytes.toString(CellUtil.cloneValue(cell));
                        */
                        String str = Bytes.toString(CellUtil.cloneValue(cell));

                        System.out.println("--------------------------------------------------------------------------------");
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
