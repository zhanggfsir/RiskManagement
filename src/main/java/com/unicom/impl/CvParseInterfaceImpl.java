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

public class CvParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.VoiceHis.Builder voiceHis = Risk.VoiceHis.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String userId = array[0];
        voiceHis.setUserId(userId);

        try { voiceHis.setThisTotalDura((int)Double.parseDouble(array[1]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisInDura((int)Double.parseDouble(array[2]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisOutDura((int)Double.parseDouble(array[3]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisLocalDura((int)Double.parseDouble(array[4]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisTollDura((int)Double.parseDouble(array[5]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamDura((int)Double.parseDouble(array[6]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvDura((int)Double.parseDouble(array[7]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvCallingDura((int)Double.parseDouble(array[8]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvCalledDura((int)Double.parseDouble(array[9]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamCounDura((int)Double.parseDouble(array[10]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounDura((int)Double.parseDouble(array[11]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounCallingD((int)Double.parseDouble(array[12]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounCalledDura((int)Double.parseDouble(array[13]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamChangtuDura((int)Double.parseDouble(array[14]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisTotalNums((int)Double.parseDouble(array[15]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisInNums((int)Double.parseDouble(array[16]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisOutNums((int)Double.parseDouble(array[17]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisLocalNums((int)Double.parseDouble(array[18]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisTollNums((int)Double.parseDouble(array[19]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamNums((int)Double.parseDouble(array[20]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvNums((int)Double.parseDouble(array[21]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvCallingNums((int)Double.parseDouble(array[22]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamProvCalledNums((int)Double.parseDouble(array[23]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamCounNums((int)Double.parseDouble(array[24]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounNums((int)Double.parseDouble(array[25]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounCallingN((int)Double.parseDouble(array[26]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamOutCounCalledNums((int)Double.parseDouble(array[27]));} catch (NumberFormatException e) {}
        try { voiceHis.setThisRoamChangtuNums((int)Double.parseDouble(array[28]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3TotalDura((int)Double.parseDouble(array[29]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3InDura((int)Double.parseDouble(array[30]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3OutDura((int)Double.parseDouble(array[31]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3LocalDura((int)Double.parseDouble(array[32]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3TollDura((int)Double.parseDouble(array[33]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamDura((int)Double.parseDouble(array[34]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvDura((int)Double.parseDouble(array[35]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvCallingDura((int)Double.parseDouble(array[36]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvCalledDura((int)Double.parseDouble(array[37]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamCounDura((int)Double.parseDouble(array[38]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounDura((int)Double.parseDouble(array[39]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounCallingD((int)Double.parseDouble(array[40]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounCalledD((int)Double.parseDouble(array[41]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamChangtuDura((int)Double.parseDouble(array[42]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3TotalNums((int)Double.parseDouble(array[43]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3InNums((int)Double.parseDouble(array[44]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3OutNums((int)Double.parseDouble(array[45]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3LocalNums((int)Double.parseDouble(array[46]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3TollNums((int)Double.parseDouble(array[47]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamNums((int)Double.parseDouble(array[48]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvNums((int)Double.parseDouble(array[49]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvCallingNums((int)Double.parseDouble(array[50]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamProvCalledNums((int)Double.parseDouble(array[51]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamCounNums((int)Double.parseDouble(array[52]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounNums((int)Double.parseDouble(array[53]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounCallingN((int)Double.parseDouble(array[54]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamOutCounCalledN((int)Double.parseDouble(array[55]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast3RoamChangtuNums((int)Double.parseDouble(array[56]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TotalDura((int)Double.parseDouble(array[57]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6InDura((int)Double.parseDouble(array[58]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6OutDura((int)Double.parseDouble(array[59]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6LocalDura((int)Double.parseDouble(array[60]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TollDura((int)Double.parseDouble(array[61]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamDura((int)Double.parseDouble(array[62]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvDura((int)Double.parseDouble(array[63]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvCallingDura((int)Double.parseDouble(array[64]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvCalledDura((int)Double.parseDouble(array[65]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamCounDura((int)Double.parseDouble(array[66]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounDura((int)Double.parseDouble(array[67]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounCallingD((int)Double.parseDouble(array[68]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounCalledD((int)Double.parseDouble(array[69]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamChangtuDura((int)Double.parseDouble(array[70]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TotalNums((int)Double.parseDouble(array[71]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6InNums((int)Double.parseDouble(array[72]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6OutNums((int)Double.parseDouble(array[73]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6LocalNums((int)Double.parseDouble(array[74]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TollNums((int)Double.parseDouble(array[75]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamNums((int)Double.parseDouble(array[76]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvNums((int)Double.parseDouble(array[77]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvCallingNums((int)Double.parseDouble(array[78]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamProvCalledNums((int)Double.parseDouble(array[79]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamCounNums((int)Double.parseDouble(array[80]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounNums((int)Double.parseDouble(array[81]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounCallingN((int)Double.parseDouble(array[82]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamOutCounCalledN((int)Double.parseDouble(array[83]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6RoamChangtuNums((int)Double.parseDouble(array[84]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TotalDuraMax((int)Double.parseDouble(array[85]));} catch (NumberFormatException e) {}
        try { voiceHis.setLast6TotalDuraCv((int)Double.parseDouble(array[86]));} catch (NumberFormatException e) {}

        buff = voiceHis.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[2].split("/")[0];
        keyValue.append(userId);
        keyValue.append(provId);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);
        // rowkey: user_id,prov_id,yyyyMM
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
