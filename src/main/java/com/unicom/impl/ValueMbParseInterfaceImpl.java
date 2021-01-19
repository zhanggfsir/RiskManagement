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
 * // 极低用户 月表
 * // 极低用户 月表
 * // 表: business_monthly_userid
 * // 列：f:value
 * // 来源: ZBA_DWA.DWA_V_M_CUS_3G_SING_VALUE、ZBA_DWA.DWA_V_M_CUS_CB_SING_VALUE
 * // 取值: is_active=1
 * // rowkey: user_id,prov_id,yyyyMM
 * message SingValue {
 *     optional int32 is_lower_user  = 1 ;
 *     optional int32 is_low_user    = 2 ;
 *     optional int32 is_high_user   = 3 ;
 *     optional float user_value     = 4 ;
 * }
 */
public class ValueMbParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        String column="value";
        Risk.SingValue.Builder singValueBuild=Risk.SingValue.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String provId=path.toString().split("=")[2].split("/")[0];
        String isLowerUser  = array[1] ;
        String isLowUser    =array[2] ;
        String isHighUser   = array[3] ;
        String userValue    = array[4] ;
        String userId=array[0];

        try{ singValueBuild.setIsLowerUser(Integer.parseInt(isLowerUser));} catch (NumberFormatException e) { }
        try{ singValueBuild.setIsLowUser(Integer.parseInt(isLowUser));} catch (NumberFormatException e) { }
        try{ singValueBuild.setIsHighUser(Integer.parseInt(isHighUser));} catch (NumberFormatException e) { }
        try{ singValueBuild.setUserValue(Float.parseFloat(userValue));} catch (NumberFormatException e) { }

        buff = singValueBuild.build().toByteArray();
        //rowkey: user_id,prov_id,yyyyMM
        StringBuilder keyValue=new StringBuilder();
        keyValue.append(userId);
        keyValue.append(provId);
        account=account.substring(0,6);
        keyValue.append(account);

        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(column), buff);
        return put;
    }
}