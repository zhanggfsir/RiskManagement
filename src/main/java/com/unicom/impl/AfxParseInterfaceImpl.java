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

public class AfxParseInterfaceImpl   implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.AfxUserInfo.Builder afxUserInfo=Risk.AfxUserInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = null;
        if (loadColumnInfo.getSeperator().equalsIgnoreCase("0x01")){
            byte  b[] = {0x01};
            array= StringUtils.splitPreserveAllTokens(str,new String(b));
        }else{
            array=StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        }

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数{}与配置文件的{}不符--->{}",array.length, loadColumnInfo.getFieldNum(), str);
            int i=0;
            for(String ss:array){
                logger.info(i+"-->"+ss+"-->"+loadColumnInfo.getSeperator());
                i++;
            }
            return null;
        }
        afxUserInfo.setAreaId(array[1]);
        afxUserInfo.setUserId(array[3]);
        afxUserInfo.setCustId(array[4]);
        afxUserInfo.setServiceType(array[5]);
        afxUserInfo.setPayMode(array[6]);
        afxUserInfo.setProductId(array[7]);
        afxUserInfo.setProductMode(array[8]);
        afxUserInfo.setInnetDate(array[9]);
        afxUserInfo.setCloseDate(array[10]);
        try{ afxUserInfo.setInnetMonths((int)Double.parseDouble(array[11])); } catch (NumberFormatException e) { }
        try{ afxUserInfo.setIsInnet(Integer.parseInt(array[12])); } catch (NumberFormatException e) { }
        try{ afxUserInfo.setIsStat(Integer.parseInt(array[13])); } catch (NumberFormatException e) { }
        afxUserInfo.setUserIdEn(array[14]);
        // DEVICE_NUMBER_EN
        afxUserInfo.setUserStatus(array[16]);
        afxUserInfo.setStopType(array[17]);
        afxUserInfo.setLastStopDate(array[18]);
        afxUserInfo.setChannelId(array[19]);
        buff = afxUserInfo.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();


        //String accout=path.toString().split("/")[7];
        // rowkey: device_number_sha256,yyyyMMdd
        String deviceNumber=array[2];
        Encryption encryption=new Encryption();
        String deviceNumberSha256=encryption.sha256(deviceNumber).toUpperCase();
        keyValue.append(deviceNumberSha256);
        keyValue.append(account);

        temp = Bytes.toBytes((short) (deviceNumberSha256.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}

