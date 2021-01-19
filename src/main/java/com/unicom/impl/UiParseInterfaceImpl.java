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

public class UiParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.AlUserInfo.Builder alUserInfoBuild=Risk.AlUserInfo.newBuilder();
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

        String provId=path.toString().split("=")[3].split("/")[0];
        alUserInfoBuild.setAreaId(array[1]);
        alUserInfoBuild.setUserId(array[2]);
        alUserInfoBuild.setServiceType(array[4]);
        alUserInfoBuild.setPayMode(array[5]);
        alUserInfoBuild.setProductId(array[6]);
        alUserInfoBuild.setProductMode(array[7]);
        alUserInfoBuild.setInnetDate(array[8]);

        try{
            alUserInfoBuild.setInnetMonths((int)Double.parseDouble(array[9]));
        } catch (NumberFormatException e) {
        }
        try{
            int isCard = Integer.parseInt(array[10]);
            alUserInfoBuild.setIsCard(isCard);
        } catch (NumberFormatException e) {
        }
        try{
            int isInnet = Integer.parseInt(array[11]);
            alUserInfoBuild.setIsInnet(isInnet);
        } catch (NumberFormatException e) {
        }
        try{
            if (!array[12].equals("")){ //不set值，为null
                alUserInfoBuild.setIsThisAcct(Integer.parseInt(array[12]));
            }
        } catch (NumberFormatException e) {
        }
        try{
            int isThisBreak= Integer.parseInt(array[13]);
            alUserInfoBuild.setIsThisBreak(isThisBreak);
        } catch (NumberFormatException e) {
        }
        alUserInfoBuild.setCloseDate(array[14]);
        alUserInfoBuild.setCustId(array[15]);
        alUserInfoBuild.setUserIdEn(array[16]);
        try{
            //alUserInfoBuild.setIsGrpMbr(Integer.parseInt(array[18]));
        } catch (NumberFormatException e) {
        }

        alUserInfoBuild.setUserStatus(array[20]);
        try{
            int isStat= Integer.parseInt(array[21]);
            alUserInfoBuild.setIsStat(isStat);
        } catch (NumberFormatException e) {
        }
        alUserInfoBuild.setChannelId(array[24]);
        alUserInfoBuild.setStopType(array[25]);
        alUserInfoBuild.setProvId(provId);

        buff = alUserInfoBuild.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        String deviceNumberMd5=array[17].toUpperCase();
        keyValue.append(deviceNumberMd5);
        keyValue.append(account);
        //手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
