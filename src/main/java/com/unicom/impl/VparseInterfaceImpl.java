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

public class VparseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String jjqStr, LoadColumnInfo loadColumnInfo, Path path, String account) {

        Risk.VoiceList.Builder voiceList=Risk.VoiceList.newBuilder();


        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] jjqItem = StringUtils.splitPreserveAllTokens(jjqStr,loadColumnInfo.getSeperator());

        if (jjqItem.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), jjqStr);
            return null;
        }

        String monthId =path.toString().split("=")[1].split("/")[0];
        String dayId =path.toString().split("=")[2].split("/")[0];
        String provId =path.toString().split("=")[3].split("/")[0];


        // String jjqItem="A|B$20190701120000,20190701130000,60,01,01AA,01AA,2.0$20190601120000,20190601130000,60,01,01AA,01AA,2.0";
        // String[] jjqItem=StringUtils.splitPreserveAllTokens(jjqStr,"|");

        //获得单条记录
        String[] callItemSingle=StringUtils.splitPreserveAllTokens(jjqItem[2],"$");
        for(String str:callItemSingle){
            Risk.VoiceDetail.Builder voiceDetail = Risk.VoiceDetail.newBuilder();
            String[] callDetail=StringUtils.splitPreserveAllTokens(str,",");
            voiceDetail.setStartTime(callDetail[0]);
            voiceDetail.setEndTime(callDetail[1]);
            voiceDetail.setMonthId(monthId);
            voiceDetail.setDayId(dayId);
            voiceDetail.setProvId(provId);
            try{  voiceDetail.setCallDuration(Float.parseFloat(callDetail[2])); } catch (NumberFormatException e) { };
            voiceDetail.setCallType(callDetail[3]);
            voiceDetail.setRoamType(callDetail[4]);
            try{  voiceDetail.setLongType(callDetail[5]);} catch(Exception e){ };

            try{  voiceDetail.setBaseTimes(Float.parseFloat(callDetail[6])); } catch (Exception e) { };
            voiceList.addVoiceDetail(voiceDetail);
        }


        buff = voiceList.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String deviceNumberMd5=jjqItem[0];
        String opposeNumberMd5=jjqItem[1];
        String date=monthId+dayId;
        keyValue.append(deviceNumberMd5);
        keyValue.append(opposeNumberMd5);
        keyValue.append(date);

       //rowkey: device_number_md5,oppose_number_md5,yyyyMMdd
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
