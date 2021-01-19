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
 * // 合约业务 月表
 * // 表: business_monthly_userid
 * // 列：f:act
 * // 来源: zba_dwa.dwa_v_d_cus_cb_act_info、zba_dwa.dwa_v_d_cus_3g_act_info、zba_dwa.dwa_v_d_cus_2g_act_info
 * // 取值: zba_dwa.dwa_v_d_cus_cb_act_info取is_valid = '1' and activity_type <> '04'
 * // 取值: zba_dwa.dwa_v_d_cus_3g_act_info取is_valid = '1' and is_card <> '1'
 * // rowkey: user_id,prov_id,yyyyMM
 * message ActInfo {
 *     optional string activity_type = 1 ; // 总部活动类型
 * }
 */
public class Act3gParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        String column="act";
        Risk.ActInfo.Builder actInfoBuild=Risk.ActInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String provId=path.toString().split("=")[3].split("/")[0];
        String userId=array[6];
        String activityType =array[20];
        String isValid=array[42];
        String isCard=array[49];
        //取is_valid = '1' and is_card <> '1'
        if(!StringUtils.equals(isValid,"1") || StringUtils.equals(isCard,"1")){
            return null;
        }
        actInfoBuild.setActivityType(activityType);

        buff = actInfoBuild.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        keyValue.append(userId);
        keyValue.append(provId);
        account=account.substring(0,6);
        keyValue.append(account);
        //rowkey: user_id,prov_id,yyyyMM
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(column), buff);
        return put;
    }
}