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
/**
 * // 用户换机历史 月表
 * // 表: imei_monthly_msisdn
 * // 列: f:five
 */
public class FiveParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.ImeiChange.Builder imeiChange = Risk.ImeiChange.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        imeiChange.setImei(array[1]);
        imeiChange.setImsi(array[2]);
        imeiChange.setFactoryId(array[3]);
        imeiChange.setTermId(array[4]);
        imeiChange.setFactoryDesc(array[5]);
        imeiChange.setTermDesc(array[6]);
        imeiChange.setCityNo(array[7]);
        imeiChange.setUpTime(array[8]);
        imeiChange.setIsCopy(array[9]);


        try { imeiChange.setUseTimes(Integer.parseInt(array[10]));} catch (NumberFormatException e) {}

        buff = imeiChange.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        Md5Util md5Util=new Md5Util();
        String deviceNumberMd5=md5Util.md5(array[0]);
        String useOrder=array[11];
        keyValue.append(deviceNumberMd5);
        keyValue.append(useOrder);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        // rowkey: device_number_md5,rn,yyyyMM
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}