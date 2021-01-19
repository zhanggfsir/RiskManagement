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
 // 漫游类型 月表
 // 表: user_monthly_msisdn
 // 列: f:roam
 // 来源: 从ZBA_DWA.DWA_S_M_USE_2G_VOICE_S、ZBA_DWA.DWA_S_M_USE_3G_VOICE_S、ZBA_DWA.DWA_S_M_USE_CB_VOICE_S加工新表入库
 // rowkey: device_number_md5,yyyyMM
 */

public class RoamParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        String column="roam";
        Risk.RoamType.Builder romaTypeBuild=Risk.RoamType.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String month=path.toString().split("=")[1].split("/")[0];

        String serviceType=array[1];
        String romaType=array[2];

        try{ romaTypeBuild.setRoamType(Integer.parseInt(romaType)); } catch (NumberFormatException e) { }
        romaTypeBuild.setServiceType(serviceType);

        buff = romaTypeBuild.build().toByteArray();
        // rowkey: device_number_md5,yyyyMM
        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMd5=array[0];
        keyValue.append(deviceNumberMd5);
        keyValue.append(month);

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(column), buff);
        return put;
    }
}