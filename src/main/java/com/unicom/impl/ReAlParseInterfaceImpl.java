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

//re_al和re_cb列 2020年2月4日经确认下线，改为由手机号查询

public class ReAlParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {

        String columnName="re";

        Risk.RnsEncap.Builder rnsEncap = Risk.RnsEncap.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=8){
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }

        rnsEncap.setCustNameSha256(array[17]);
        rnsEncap.setCertNoSha256(array[18]);

        buff = rnsEncap.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String provId = path.toString().split("=")[3].split("/")[0];
        String custId=array[3];
        keyValue.append(custId);
        keyValue.append(provId);
        keyValue.append(account);
        // rowkey: cust_id,prov_id,yyyyMMdd
        temp = Bytes.toBytes((short) (custId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(columnName), buff);
        return put;

    }
}
