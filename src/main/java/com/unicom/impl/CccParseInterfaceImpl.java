package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.inter.QueryBatchInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CccParseInterfaceImpl implements ParseInterface{
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.CustCbCharge.Builder custCbCharge = Risk.CustCbCharge.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
//        System.out.println();
//        for(int i = 0; i < array.length;i++) {
//            System.out.print(array[i]);
//            System.out.print("--");
//        }
        String cust_id=array[0];
        custCbCharge.setCustId(cust_id);
        custCbCharge.setThisFee(StringUtils.isBlank(array[1]) ? 0 : (int) Double.parseDouble(array[1]));
        custCbCharge.setLast1Fee(StringUtils.isBlank(array[2]) ? 0 : (int)Double.parseDouble(array[2]));
        custCbCharge.setLast2Fee(StringUtils.isBlank(array[3]) ? 0 : (int)Double.parseDouble(array[3]));
        custCbCharge.setLast3Fee(StringUtils.isBlank(array[4]) ? 0 : (int)Double.parseDouble(array[4]));
        custCbCharge.setLast4Fee(StringUtils.isBlank(array[5]) ? 0 : (int)Double.parseDouble(array[5]));
        custCbCharge.setLast5Fee(StringUtils.isBlank(array[6]) ? 0 : (int)Double.parseDouble(array[6]));
        custCbCharge.setLast6Fee(StringUtils.isBlank(array[7]) ? 0 : (int)Double.parseDouble(array[7]));
        custCbCharge.setLast7Fee(StringUtils.isBlank(array[8]) ? 0 : (int)Double.parseDouble(array[8]));
        custCbCharge.setLast8Fee(StringUtils.isBlank(array[9]) ? 0 : (int)Double.parseDouble(array[9]));
        custCbCharge.setLast9Fee(StringUtils.isBlank(array[10]) ? 0 : (int)Double.parseDouble(array[10]));
        custCbCharge.setLast10Fee(StringUtils.isBlank(array[11]) ? 0 : (int)Double.parseDouble(array[11]));
        custCbCharge.setLast11Fee(StringUtils.isBlank(array[12]) ? 0 : (int)Double.parseDouble(array[12]));
        custCbCharge.setTotalYearFee(StringUtils.isBlank(array[13]) ? 0 : (int)Double.parseDouble(array[13]));

        buff = custCbCharge.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[2].split("/")[0];
        keyValue.append(cust_id);
        keyValue.append(provId);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        //cust_id 哈希，cust_id,prov_id,yyyyMM作为key
        temp = Bytes.toBytes((short) (cust_id.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
