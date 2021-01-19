package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.MsisdnSeg.Builder msisdnSeg=Risk.MsisdnSeg.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        msisdnSeg.setServType(array[1]);
        msisdnSeg.setAreaCode(array[2]);
        msisdnSeg.setProvName(array[3]);
        msisdnSeg.setAreaName(array[4]);
        msisdnSeg.setProvId(array[5]);
        msisdnSeg.setAreaId(array[6]);


        buff = msisdnSeg.build().toByteArray();

        String deviceNo=array[0];
        // rowkey: device_no // 号段
        temp = Bytes.toBytes((short) (deviceNo.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(deviceNo));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
