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
 // 融合业务 月表
 // 表: business_monthly_userid
 // 列：f:comp
 // 来源: zba_dwa.dwa_v_m_cus_cb_rh_mem_wit、zba_dwa.dwa_v_m_cus_al_ord_member
 // 取值: zba_dwa.dwa_v_m_cus_cb_rh_mem_wit取 comp_type in ('81', '82', '83') and is_comp_valid = '1' and is_mem_valid = '1'
 // 取值: zba_dwa.dwa_v_m_cus_al_ord_member取 comp_type in ('61', '62', '67') and is_comp_valid = '1' and is_valid = '1'
 // rowkey: user_id,prov_id,yyyyMM
 message CompInfo {
 optional string comp_type = 1 ; // 融合类型
 }
 */
public class CompCbParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        String column="comp";
        Risk.CompInfo.Builder compInfoBuild=Risk.CompInfo.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

        String provId=path.toString().split("=")[2].split("/")[0];
        String compType=array[4];
        compInfoBuild.setCompType(compType);
        //comp_type in ('81', '82', '83') and is_comp_valid = '1' and is_mem_valid = '1'
        String isCompValid =array[21];
        String isMemValid=array[22];
        String userId=array[3];
        if(!(StringUtils.equals(compType,"81")||StringUtils.equals(compType,"82")||StringUtils.equals(compType,"83"))
                || !StringUtils.equals(isCompValid,"1") || !StringUtils.equals(isMemValid,"1")){
            return null;
        }

        buff = compInfoBuild.build().toByteArray();

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