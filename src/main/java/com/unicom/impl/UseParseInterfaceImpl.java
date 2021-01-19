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
 * // 20191101 新增
 * // 用户价值 月表
 * // 功能: 可判断用户最后一次活跃月份、是否呼叫转移、是否漫游、三无
 * // 表: business_monthly_userid
 * // 列：f:use
 * // 来源: zba_dwa.DWA_V_M_CUS_NM_SING_USE
 * // 取值: is_active=1
 * // rowkey: user_id,prov_id,yyyyMM //user_id使用旧系统user_id_old
 * message NmSingUse {
 *     optional int32 is_change_call = 1 ; // 是否呼叫转移
 *     optional int32 is_roma_call   = 2 ; // 是否漫游
 * // 0	三无
 * // 1	只使用语音
 * // 2	只使用短信
 * // 3	只使用流量
 * // 4	使用语音和短信
 * // 5	使用语音和流量
 * // 6	使用短信和流量
 * // 7	使用语音、短息及流量
 * // 9	其他
 *     optional string use_status    = 3 ; // 使用状态
 * }
 */
public class UseParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.NmSingUse.Builder nmSingUseBuild=Risk.NmSingUse.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String provId=path.toString().split("=")[2].split("/")[0];;
        //取值: is_active=1
        String isActive=array[11];
        if(!StringUtils.equals(isActive,"1"))
            return  null;

        try{nmSingUseBuild.setIsChangeCall(Integer.parseInt(array[7])); } catch (NumberFormatException e) { }
        try{nmSingUseBuild.setIsRomaCall(Integer.parseInt(array[6])); } catch (NumberFormatException e) { }
        nmSingUseBuild.setUseStatus(array[15]);

        buff = nmSingUseBuild.build().toByteArray();
        //rowkey: user_id,prov_id,yyyyMM
        StringBuilder keyValue=new StringBuilder();
        String userId=array[16];
        keyValue.append(userId);
        keyValue.append(provId);
        keyValue.append(account);
        temp = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
