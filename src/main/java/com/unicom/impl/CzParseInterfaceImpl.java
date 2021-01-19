package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.unicom.risk.Risk;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;

public class CzParseInterfaceImpl   implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.CzInfo.Builder czInfo = Risk.CzInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
//        String provId=path.toString().split("=")[3].split("/")[0];
        czInfo.setProvId(array[1]);
        czInfo.setAreaId(array[2]);
        try { czInfo.setDays(Integer.parseInt(array[3]));} catch (NumberFormatException e) {}

        buff = czInfo.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String deviceNumberMd5=array[0].toUpperCase();
        String rn=array[4];
        keyValue.append(deviceNumberMd5);
        keyValue.append(rn);
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
