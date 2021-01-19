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

public class MfeeParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.FeeHis.Builder feeHis = Risk.FeeHis.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String userId = array[0];
        feeHis.setUserId(userId);

        try { feeHis.setThisFee((int)Double.parseDouble(array[3]));} catch (NumberFormatException e) {}
        try { feeHis.setLast1Fee((int)Double.parseDouble(array[4]));} catch (NumberFormatException e) {}
        try { feeHis.setLast2Fee((int)Double.parseDouble(array[5]));} catch (NumberFormatException e) {}
        try { feeHis.setLast3Fee((int)Double.parseDouble(array[6]));} catch (NumberFormatException e) {}
        try { feeHis.setLast4Fee((int)Double.parseDouble(array[7]));} catch (NumberFormatException e) {}
        try { feeHis.setLast5Fee((int)Double.parseDouble(array[8]));} catch (NumberFormatException e) {}
        try { feeHis.setLast6Fee((int)Double.parseDouble(array[9]));} catch (NumberFormatException e) {}
        try { feeHis.setLast7Fee((int)Double.parseDouble(array[10]));} catch (NumberFormatException e) {}
        try { feeHis.setLast8Fee((int)Double.parseDouble(array[11]));} catch (NumberFormatException e) {}
        try { feeHis.setLast9Fee((int)Double.parseDouble(array[12]));} catch (NumberFormatException e) {}
        try { feeHis.setLast10Fee((int)Double.parseDouble(array[13]));} catch (NumberFormatException e) {}
        try { feeHis.setLast11Fee((int)Double.parseDouble(array[14]));} catch (NumberFormatException e) {}
        try { feeHis.setTotalYearFee((int)Double.parseDouble(array[15]));} catch (NumberFormatException e) {}

        buff = feeHis.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[2].split("/")[0];;
        keyValue.append(userId);
        keyValue.append(provId);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        // user_id 哈希，user_id,prov_id,yyyyMM作为key
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;

    }
}
