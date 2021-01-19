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

public class LaParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {

        Risk.LastActive.Builder lastActive = Risk.LastActive.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=6){
            logger.error("输入账期的格式为 yyyyMM,当前输入账期为{},不符合规范", account);
            return null;
        }
        String monthId =path.toString().split("=")[1].split("/")[0];
        String provId=path.toString().split("=")[2].split("/")[0];
        String userId=array[16];
        int isActive = StringUtils.isBlank(array[11])?0:Integer.valueOf(array[11]);
        if(isActive == 0){
            return null;
        }
        lastActive.setMonthId(Integer.valueOf(monthId));

        buff = lastActive.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        //rowkey: user_id,prov_id
        keyValue.append(userId);
        keyValue.append(provId);
        // rowkey: userId,provId
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;

    }
}
