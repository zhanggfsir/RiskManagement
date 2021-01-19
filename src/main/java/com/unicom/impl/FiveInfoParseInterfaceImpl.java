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

/**
 * // 五元组 日表 T-3
 * // 表: user_daily_msisdn
 * // 列: f:five
 */
public class FiveInfoParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.FiveInfo.Builder fiveInfo = Risk.FiveInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        fiveInfo.setImsi(array[1]);
        fiveInfo.setImei(array[2]);
        fiveInfo.setImei15(array[3]);
        fiveInfo.setFactoryId(array[4]);
        fiveInfo.setTermId(array[5]);
        fiveInfo.setFactoryDesc(array[6]);
        fiveInfo.setTermDesc(array[7]);
        fiveInfo.setUpTime(array[8]);


        buff = fiveInfo.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String deviceNumberMd5= Encryption.md5(array[0]);

        keyValue.append(deviceNumberMd5);
        keyValue.append(account);
        // rowkey: device_number_md5,yyyyMMdd
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}