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

/**
 * // 欠费信息 月表
 * // 表: business_monthly_userid
 * // 列：f:owe
 * // 来源: 从zba_dwd.dwd_m_acc_al_owe_finace、zba_dwa.dwa_s_m_acc_cb_owe 加工新表入库 zba_dwa.dwa_m_acc_al_owe
 * // rowkey: user_id,prov_id,yyyyMM
 * message OweInfo {
 *     required float owe_fee  = 1 ;
 * }
 */
public class OweParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.OweInfo.Builder oweInfoBuild=Risk.OweInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String provId=path.toString().split("=")[2].split("/")[0];

        try{ oweInfoBuild.setOweFee(Float.parseFloat(array[5])); } catch (NumberFormatException e) { }

        buff = oweInfoBuild.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String user_id=array[1];
        keyValue.append(user_id);
        keyValue.append(provId);
        account=account.substring(0,6);
        keyValue.append(account);
        //rowkey: user_id,prov_id,yyyyMM
        temp = Bytes.toBytes((short) (user_id.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}