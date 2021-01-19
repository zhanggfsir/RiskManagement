package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import com.unicom.utils.Encryption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PCityList14dayImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(PCityList14dayImpl.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.CityList14Day.Builder cityList14Day=Individualization.CityList14Day.newBuilder();
        Individualization.CityList.Builder cityList=Individualization.CityList.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String cityList14DaySeparatorChar="|";
        String cityListSeparatorChar="_";
        String[] cityList14DayArr = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(cityList14DayArr.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        // TODO md5
        Encryption encryption=new Encryption();
        String deviceNumberMd5=encryption.md5(cityList14DayArr[0]) ;

        String month=path.toString().split("=")[1].split("/")[0];
        String day=path.toString().split("=")[2].split("/")[0];
        String yyyyMMdd=month+day; // 拼接
        String cityList14DayStr=cityList14DayArr[1]; // 城市列表 city_list
        String[] cityListItem=StringUtils.splitPreserveAllTokens(cityList14DayStr,cityList14DaySeparatorChar);
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
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}

