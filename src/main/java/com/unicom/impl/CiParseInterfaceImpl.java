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

public class CiParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.AlCustInfo.Builder alCustInfo=Risk.AlCustInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=8){
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }
        alCustInfo.setCertType(array[1]);

        try{
            alCustInfo.setCustSex(Integer.parseInt(array[2]));
        } catch (NumberFormatException e) { }

        try{
            alCustInfo.setCertAge((int)Double.parseDouble(array[3]));
        } catch (NumberFormatException e) { }

        alCustInfo.setConstellationDesc(array[4]);
        alCustInfo.setCustBirthday(array[5]);
        alCustInfo.setCustNameMd5(array[6]);
        alCustInfo.setCertNoMd5(array[7]);
        alCustInfo.setNameMosaic(array[8]);

        try{
            alCustInfo.setCertUsernums((int)Double.parseDouble(array[9]));
        } catch (NumberFormatException e) { }

        try{
            alCustInfo.setCertInnetUsernums((int)Double.parseDouble(array[10]));
        } catch (NumberFormatException e) { }

        try{
            alCustInfo.setCertBreakUsernums((int)Double.parseDouble(array[11]));
        } catch (NumberFormatException e) { }


        buff = alCustInfo.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMd5=array[0].toUpperCase();
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
