package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Md5Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HwParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.UserNmPermanent.Builder userNmPermanent=Risk.UserNmPermanent.newBuilder();
        Risk.Point.Builder point=Risk.Point.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        userNmPermanent.setProvId(array[1]);

//        top1_work
        try {
            point.setLongitude(Float.parseFloat(array[7]));
            point.setLatitude(Float.parseFloat(array[8]));
            point.setProvince(array[4]);
            point.setCity(array[5]);
            point.setZone(array[6]);
            userNmPermanent.setTop1Work(point);
        } catch (Exception e) {
        }
        point.clear();

//      top2_work
        try {
            point.setLongitude(Float.parseFloat(array[12]));
            point.setLatitude(Float.parseFloat(array[13]));
            point.setProvince(array[9]);
            point.setCity(array[10]);
            point.setZone(array[11]);
            userNmPermanent.setTop2Work(point);
        } catch (Exception e) {
        }
        point.clear();

//      top3_work
        try {
            point.setLongitude(Float.parseFloat(array[17]));
            point.setLatitude(Float.parseFloat(array[18]));
            point.setProvince(array[14]);
            point.setCity(array[15]);
            point.setZone(array[16]);
            userNmPermanent.setTop3Work(point);
        } catch (Exception e) {
        }
        point.clear();
//        top1_home
        try {
            point.setLongitude(Float.parseFloat(array[22]));
            point.setLatitude(Float.parseFloat(array[23]));
            point.setProvince(array[19]);
            point.setCity(array[20]);
            point.setZone(array[21]);
            userNmPermanent.setTop1Home(point);
        } catch (Exception e) {
        }
        point.clear();

//      top2_home
        try {
            point.setLongitude(Float.parseFloat(array[27]));
            point.setLatitude(Float.parseFloat(array[28]));
            point.setProvince(array[24]);
            point.setCity(array[25]);
            point.setZone(array[26]);
            userNmPermanent.setTop2Home(point);
        } catch (Exception e) {
        }
        point.clear();
//      top3_home
        try {
            point.setLongitude(Float.parseFloat(array[32]));
            point.setLatitude(Float.parseFloat(array[33]));
            point.setProvince(array[29]);
            point.setCity(array[30]);
            point.setZone(array[31]);
            userNmPermanent.setTop3Home(point);
        } catch (Exception e) {
        }
        point.clear();


        buff = userNmPermanent.build().toByteArray();

        //拼接rowkey  rowkey: device_number_md5,yyyyMM
        StringBuilder keyValue=new StringBuilder();
        Md5Util md5Util=new Md5Util();
        String deviceNumberMd5=md5Util.md5(array[3]);
        keyValue.append(deviceNumberMd5);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        //手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
