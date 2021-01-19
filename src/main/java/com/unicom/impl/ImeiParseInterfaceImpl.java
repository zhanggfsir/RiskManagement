package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Encryption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImeiParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.Imei2Msisdn.Builder imei2Msisdn = Risk.Imei2Msisdn.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str, loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        if (account.length() != 8) {
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }

        /*for (int i=0;i<array.length;i++){
            System.out.print(i+":"+array[i]+"\t");
        }
        System.out.println();*/
        String deviceNumberMd5= Encryption.md5(array[0]);
        imei2Msisdn.setDeviceNumberMd5(deviceNumberMd5);
        imei2Msisdn.setUpTime(array[8]);
        buff = imei2Msisdn.build().toByteArray();


        StringBuilder keyValue = new StringBuilder();
        if (StringUtils.isBlank(array[2])){
            return null;
        }
        String imei=Encryption.md5(array[2]).toUpperCase();
        keyValue.append(imei);
        keyValue.append(account);
        // rowkey: imsi,yyyyMMdd
        temp = Bytes.toBytes((short) (imei.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
