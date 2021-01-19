package com.unicom.tools;

import com.unicom.entity.ZkInfo;
import com.unicom.risk.Individualization;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.Encryption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.CityList14daySingleDataUpdate 319 17610001153 20200215 011_V0110000_1101005#051_V0440300_440311
// java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.CityList14daySingleDataUpdate 319 262CA5841FCF3D2936646608F90AEE61 20200215 011_V0110000_1101005#051_V0440300_440311


/**
 * zkName=args[0];        319 or 419
 * deviceNumber=args[1];  手机号明文,或者md5值
 * yyyyMMdd=args[2];      账期8位
 * str=args[3];           要更改的字段 样例
 *                                         只有一条驻留情况 011_V0110000_1101005
 *                                         当有多条驻留情况 011_V0110000_1101005#051_V0440300_440311
 *  011_V0110000_1101005 解释 prov_area_city 即 发生省份编码_发生地市编码_发生区县编码
 * 来源于表 ubd_serv_tour.serv_d_cus_tour_al_location_stay
 * 如果对于省地市区县编码不清楚，见信令码表 ubd_x_dim.dim_xzqh_final
 */
public class CityList14daySingleDataUpdate {
    private static Logger logger = LoggerFactory.getLogger(CityList14daySingleDataUpdate.class);
    public static void main(String[] args) {

        //1. 得到 table链接对象
        // 获得hbase连接池
        String zkName=args[0]; // 319 419
        String deviceNumber=args[1];
        String yyyyMMdd=args[2];
        String str=args[3];

        // 对手机号进行处理 目标：对传入的明文或者md5值同时支持
        // TODO md5
        String deviceNumberMd5=null;
        if(deviceNumber.length()==11){
            Encryption encryption=new Encryption();
             deviceNumberMd5=encryption.md5(deviceNumber) ;
        }else if(deviceNumber.length()==32){
            deviceNumberMd5=deviceNumber.toUpperCase();
        }else{
            System.out.println("unsupport device_number,please check the length of device_number");
        }

        String tableName="p_city_list_14day";
        String familyName="f";
        String columnName="q";
        String cityList14DaySeparatorChar="#";
        String cityListSeparatorChar="_";

        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkInfo.getZkQuorum());
        configuration.set("zookeeper.znode.parent", zkInfo.getZkParent());
        Connection connection = null;
        Table table=null;

        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2.拼接
        Individualization.CityList14Day.Builder cityList14Day=Individualization.CityList14Day.newBuilder();
        Individualization.CityList.Builder cityList=Individualization.CityList.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String cityList14DayStr=str; // 城市列表 city_list
        String[] cityListItem= StringUtils.splitPreserveAllTokens(cityList14DayStr,cityList14DaySeparatorChar);
        int i=0;
        for (String cityListStr:cityListItem){
            String cityListArr[]=StringUtils.splitPreserveAllTokens(cityListStr,cityListSeparatorChar);
            try{  cityList.setProv(cityListArr[0]); } catch (Exception e) { };
            try{  cityList.setArea(cityListArr[1]); } catch (Exception e) { };
            try{  cityList.setCity(cityListArr[2]); } catch (Exception e) { };
//            cityList14Day.setCityList(i,cityList);
            cityList14Day.addCityList(cityList);
        }

        buff = cityList14Day.build().toByteArray();
        // rowkey: device_number_md5,yyyyMMdd //手机号md5,账期
        StringBuilder keyValue=new StringBuilder();
        keyValue.append(deviceNumberMd5);
        keyValue.append(yyyyMMdd);//model_label

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(familyName), Bytes.toBytes(columnName), buff);

        // put 到hbase
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("-----------zkName:"+zkName+"put over success-----------");

    }
}
