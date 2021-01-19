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

public class CfParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.FluxHis.Builder fluxHis = Risk.FluxHis.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        String userId = array[0];
        fluxHis.setUserId(userId);
        try {
            fluxHis.setThisFlux((int)Double.parseDouble(array[1]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setThisLocalFlux((int)Double.parseDouble(array[2]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setThisRoamProvFlux((int)Double.parseDouble(array[3]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setThisRoamContFlux((int)Double.parseDouble(array[4]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setThisRoamGatFlux((int)Double.parseDouble(array[5]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setThisRoamIntFlux((int)Double.parseDouble(array[6]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3Flux((int)Double.parseDouble(array[7]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3LocalFlux((int)Double.parseDouble(array[8]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3RoamProvFlux((int)Double.parseDouble(array[9]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3RoamContFlux((int)Double.parseDouble(array[10]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3RoamGatFlux((int)Double.parseDouble(array[11]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast3RoamIntFlux((int)Double.parseDouble(array[12]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6Flux((int)Double.parseDouble(array[13]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6LocalFlux((int)Double.parseDouble(array[14]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6RoamProvFlux((int)Double.parseDouble(array[15]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6RoamContFlux((int)Double.parseDouble(array[16]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6RoamGatFlux((int)Double.parseDouble(array[17]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6RoamIntFlux((int)Double.parseDouble(array[18]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6FluxMax((int)Double.parseDouble(array[19]));
        } catch (NumberFormatException e) {
        }

        try {
            fluxHis.setLast6FluxCv((int)Double.parseDouble(array[20]));
        } catch (NumberFormatException e) {
        }


        buff = fluxHis.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[2].split("/")[0];
        keyValue.append(userId);
        keyValue.append(provId);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        //user_id 哈希，user_id,prov_id,yyyyMM作为key
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}