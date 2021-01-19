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

public class MvParseInterfaceImpl  implements ParseInterface {
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
        voiceHis.setThisTotalDura(StringUtils.isBlank(array[1]) ? 0 : (int)Double.parseDouble(array[1]));
        voiceHis.setThisInDura(StringUtils.isBlank(array[2]) ? 0 : (int)Double.parseDouble(array[2]));
        voiceHis.setThisOutDura(StringUtils.isBlank(array[3]) ? 0 : (int)Double.parseDouble(array[3]));
        voiceHis.setThisLocalDura(StringUtils.isBlank(array[4]) ? 0 : (int)Double.parseDouble(array[4]));
        voiceHis.setThisTollDura(StringUtils.isBlank(array[5]) ? 0 : (int)Double.parseDouble(array[5]));
        voiceHis.setThisRoamDura(StringUtils.isBlank(array[6]) ? 0 : (int)Double.parseDouble(array[6]));
        voiceHis.setThisRoamProvDura(StringUtils.isBlank(array[7]) ? 0 : (int)Double.parseDouble(array[7]));
        //新增 bug修复
        voiceHis.setThisRoamProvCallingDura(StringUtils.isBlank(array[8]) ? 0 : (int)Double.parseDouble(array[8]));
        voiceHis.setThisRoamProvCalledDura(StringUtils.isBlank(array[9]) ? 0 : (int)Double.parseDouble(array[9]));
        voiceHis.setThisRoamCounDura(StringUtils.isBlank(array[10]) ? 0 : (int)Double.parseDouble(array[10]));
        voiceHis.setThisRoamOutCounDura(StringUtils.isBlank(array[11]) ? 0 : (int)Double.parseDouble(array[11]));
        voiceHis.setThisRoamOutCounCallingD(StringUtils.isBlank(array[12]) ? 0 : (int)Double.parseDouble(array[12]));
        voiceHis.setThisRoamOutCounCalledDura(StringUtils.isBlank(array[13]) ? 0 : (int)Double.parseDouble(array[13]));
        voiceHis.setThisRoamChangtuDura(StringUtils.isBlank(array[14]) ? 0 : (int)Double.parseDouble(array[14]));
        voiceHis.setThisTotalNums(StringUtils.isBlank(array[15]) ? 0 : (int)Double.parseDouble(array[15]));
        voiceHis.setThisInNums(StringUtils.isBlank(array[16]) ? 0 : (int)Double.parseDouble(array[16]));
        voiceHis.setThisOutNums(StringUtils.isBlank(array[17]) ? 0 : (int)Double.parseDouble(array[17]));
        voiceHis.setThisLocalNums(StringUtils.isBlank(array[18]) ? 0 : (int)Double.parseDouble(array[18]));
        voiceHis.setThisTollNums(StringUtils.isBlank(array[19]) ? 0 : (int)Double.parseDouble(array[19]));
        voiceHis.setThisRoamNums(StringUtils.isBlank(array[20]) ? 0 : (int)Double.parseDouble(array[20]));
        voiceHis.setThisRoamProvNums(StringUtils.isBlank(array[21]) ? 0 : (int)Double.parseDouble(array[21]));
        voiceHis.setThisRoamProvCallingNums(StringUtils.isBlank(array[22]) ? 0 : (int)Double.parseDouble(array[22]));
        voiceHis.setThisRoamProvCalledNums(StringUtils.isBlank(array[23]) ? 0 : (int)Double.parseDouble(array[23]));
        voiceHis.setThisRoamCounNums(StringUtils.isBlank(array[24]) ? 0 : (int)Double.parseDouble(array[24]));
        voiceHis.setThisRoamOutCounNums(StringUtils.isBlank(array[25]) ? 0 : (int)Double.parseDouble(array[25]));
        voiceHis.setThisRoamOutCounCallingN(StringUtils.isBlank(array[26]) ? 0 : (int)Double.parseDouble(array[26]));
        voiceHis.setThisRoamOutCounCalledNums(StringUtils.isBlank(array[27]) ? 0 : (int)Double.parseDouble(array[27]));
        voiceHis.setThisRoamChangtuNums(StringUtils.isBlank(array[28]) ? 0 : (int)Double.parseDouble(array[28]));
        voiceHis.setLast3TotalDura(StringUtils.isBlank(array[29]) ? 0 : (int)Double.parseDouble(array[29]));
        voiceHis.setLast3InDura(StringUtils.isBlank(array[30]) ? 0 : (int)Double.parseDouble(array[30]));
        voiceHis.setLast3OutDura(StringUtils.isBlank(array[31]) ? 0 : (int)Double.parseDouble(array[31]));
        voiceHis.setLast3LocalDura(StringUtils.isBlank(array[32]) ? 0 : (int)Double.parseDouble(array[32]));
        voiceHis.setLast3TollDura(StringUtils.isBlank(array[33]) ? 0 : (int)Double.parseDouble(array[33]));
        voiceHis.setLast3RoamDura(StringUtils.isBlank(array[34]) ? 0 : (int)Double.parseDouble(array[34]));
        voiceHis.setLast3RoamProvDura(StringUtils.isBlank(array[35]) ? 0 : (int)Double.parseDouble(array[35]));
        voiceHis.setLast3RoamProvCallingDura(StringUtils.isBlank(array[36]) ? 0 : (int)Double.parseDouble(array[36]));
        voiceHis.setLast3RoamProvCalledDura(StringUtils.isBlank(array[37]) ? 0 : (int)Double.parseDouble(array[37]));
        voiceHis.setLast3RoamCounDura(StringUtils.isBlank(array[38]) ? 0 : (int)Double.parseDouble(array[38]));
        voiceHis.setLast3RoamOutCounDura(StringUtils.isBlank(array[39]) ? 0 : (int)Double.parseDouble(array[39]));
        voiceHis.setLast3RoamOutCounCallingD(StringUtils.isBlank(array[40]) ? 0 : (int)Double.parseDouble(array[40]));
        voiceHis.setLast3RoamOutCounCalledD(StringUtils.isBlank(array[41]) ? 0 : (int)Double.parseDouble(array[41]));
        voiceHis.setLast3RoamChangtuDura(StringUtils.isBlank(array[42]) ? 0 : (int)Double.parseDouble(array[42]));
        voiceHis.setLast3TotalNums(StringUtils.isBlank(array[43]) ? 0 : (int)Double.parseDouble(array[43]));
        voiceHis.setLast3InNums(StringUtils.isBlank(array[44]) ? 0 : (int)Double.parseDouble(array[44]));
        voiceHis.setLast3OutNums(StringUtils.isBlank(array[45]) ? 0 : (int)Double.parseDouble(array[45]));
        voiceHis.setLast3LocalNums(StringUtils.isBlank(array[46]) ? 0 : (int)Double.parseDouble(array[46]));
        voiceHis.setLast3TollNums(StringUtils.isBlank(array[47]) ? 0 : (int)Double.parseDouble(array[47]));
        voiceHis.setLast3RoamNums(StringUtils.isBlank(array[48]) ? 0 : (int)Double.parseDouble(array[48]));
        voiceHis.setLast3RoamProvNums(StringUtils.isBlank(array[49]) ? 0 : (int)Double.parseDouble(array[49]));
        voiceHis.setLast3RoamProvCallingNums(StringUtils.isBlank(array[50]) ? 0 : (int)Double.parseDouble(array[50]));
        voiceHis.setLast3RoamProvCalledNums(StringUtils.isBlank(array[51]) ? 0 : (int)Double.parseDouble(array[51]));
        voiceHis.setLast3RoamCounNums(StringUtils.isBlank(array[52]) ? 0 : (int)Double.parseDouble(array[52]));
        voiceHis.setLast3RoamOutCounNums(StringUtils.isBlank(array[53]) ? 0 : (int)Double.parseDouble(array[53]));
        voiceHis.setLast3RoamOutCounCallingN(StringUtils.isBlank(array[54]) ? 0 : (int)Double.parseDouble(array[54]));
        voiceHis.setLast3RoamOutCounCalledN(StringUtils.isBlank(array[55]) ? 0 : (int)Double.parseDouble(array[55]));
        voiceHis.setLast3RoamChangtuNums(StringUtils.isBlank(array[56]) ? 0 : (int)Double.parseDouble(array[56]));
        voiceHis.setLast6TotalDura(StringUtils.isBlank(array[57]) ? 0 : (int)Double.parseDouble(array[57]));
        voiceHis.setLast6InDura(StringUtils.isBlank(array[58]) ? 0 : (int)Double.parseDouble(array[58]));
        voiceHis.setLast6OutDura(StringUtils.isBlank(array[59]) ? 0 : (int)Double.parseDouble(array[59]));
        voiceHis.setLast6LocalDura(StringUtils.isBlank(array[60]) ? 0 : (int)Double.parseDouble(array[60]));
        voiceHis.setLast6TollDura(StringUtils.isBlank(array[61]) ? 0 : (int)Double.parseDouble(array[61]));
        voiceHis.setLast6RoamDura(StringUtils.isBlank(array[62]) ? 0 : (int)Double.parseDouble(array[62]));
        voiceHis.setLast6RoamProvDura(StringUtils.isBlank(array[63]) ? 0 : (int)Double.parseDouble(array[63]));
        voiceHis.setLast6RoamProvCallingDura(StringUtils.isBlank(array[64]) ? 0 : (int)Double.parseDouble(array[64]));
        voiceHis.setLast6RoamProvCalledDura(StringUtils.isBlank(array[65]) ? 0 : (int)Double.parseDouble(array[65]));
        voiceHis.setLast6RoamCounDura(StringUtils.isBlank(array[66]) ? 0 : (int)Double.parseDouble(array[66]));
        voiceHis.setLast6RoamOutCounDura(StringUtils.isBlank(array[67]) ? 0 : (int)Double.parseDouble(array[67]));
        voiceHis.setLast6RoamOutCounCallingD(StringUtils.isBlank(array[68]) ? 0 : (int)Double.parseDouble(array[68]));
        voiceHis.setLast6RoamOutCounCalledD(StringUtils.isBlank(array[69]) ? 0 : (int)Double.parseDouble(array[69]));
        voiceHis.setLast6RoamChangtuDura(StringUtils.isBlank(array[70]) ? 0 : (int)Double.parseDouble(array[70]));
        voiceHis.setLast6TotalNums(StringUtils.isBlank(array[71]) ? 0 : (int)Double.parseDouble(array[71]));
        voiceHis.setLast6InNums(StringUtils.isBlank(array[72]) ? 0 : (int)Double.parseDouble(array[72]));
        voiceHis.setLast6OutNums(StringUtils.isBlank(array[73]) ? 0 : (int)Double.parseDouble(array[73]));
        voiceHis.setLast6LocalNums(StringUtils.isBlank(array[74]) ? 0 : (int)Double.parseDouble(array[74]));
        voiceHis.setLast6TollNums(StringUtils.isBlank(array[75]) ? 0 : (int)Double.parseDouble(array[75]));
        voiceHis.setLast6RoamNums(StringUtils.isBlank(array[76]) ? 0 : (int)Double.parseDouble(array[76]));
        voiceHis.setLast6RoamProvNums(StringUtils.isBlank(array[77]) ? 0 : (int)Double.parseDouble(array[77]));
        voiceHis.setLast6RoamProvCallingNums(StringUtils.isBlank(array[78]) ? 0 : (int)Double.parseDouble(array[78]));
        voiceHis.setLast6RoamProvCalledNums(StringUtils.isBlank(array[79]) ? 0 : (int)Double.parseDouble(array[79]));
        voiceHis.setLast6RoamCounNums(StringUtils.isBlank(array[80]) ? 0 : (int)Double.parseDouble(array[80]));
        voiceHis.setLast6RoamOutCounNums(StringUtils.isBlank(array[81]) ? 0 : (int)Double.parseDouble(array[81]));
        voiceHis.setLast6RoamOutCounCallingN(StringUtils.isBlank(array[82]) ? 0 : (int)Double.parseDouble(array[82]));
        voiceHis.setLast6RoamOutCounCalledN(StringUtils.isBlank(array[83]) ? 0 : (int)Double.parseDouble(array[83]));
        voiceHis.setLast6RoamChangtuNums(StringUtils.isBlank(array[84]) ? 0 : (int)Double.parseDouble(array[84]));
        voiceHis.setLast6TotalDuraMax(StringUtils.isBlank(array[85]) ? 0 : (int)Double.parseDouble(array[85]));
        voiceHis.setLast6TotalDuraCv(StringUtils.isBlank(array[86]) ? 0 : (int)Double.parseDouble(array[86]));


        buff = voiceHis.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[2].split("/")[0];
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
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
