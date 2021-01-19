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

public class StopParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.StopInfn.Builder stopInfn=Risk.StopInfn.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        try{stopInfn.setIsThisStop(Integer.parseInt(array[1])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis01Cnt(Integer.parseInt(array[2])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis02Cnt(Integer.parseInt(array[3])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis03Cnt(Integer.parseInt(array[4])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis04Cnt(Integer.parseInt(array[5])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis05Cnt(Integer.parseInt(array[6])); } catch (NumberFormatException e) { }
        try{stopInfn.setStopHis06Cnt(Integer.parseInt(array[7])); } catch (NumberFormatException e) { }


        buff = stopInfn.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        String deviceNumberMd5=array[0].toUpperCase();
        keyValue.append(deviceNumberMd5);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        // 手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}

