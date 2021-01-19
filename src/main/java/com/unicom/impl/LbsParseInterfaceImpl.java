package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Md5Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // 历史位置验证 月表
 * // 表: user_monthly_msisdn
 * // 列: f:lbs
 * // 来源: DWD_D_USE_MB_VOICE、DWD_D_USE_CB_VOICE
 * // B域表 zba_dwa.dwa_d_use_al_voice
 * // rowkey: device_number_md5,yyyyMMdd,hhmmss  // 主叫年月日时分秒
 * message LBS {
 *     optional string prov_id = 1 ; // 省份
 *     optional string area_id = 2 ; // 地市
 *     optional string mcc     = 3 ; // 国家码
 * }
 */
public class LbsParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        String column="lbs";
        Risk.LBS.Builder lBSBuild=Risk.LBS.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        //String[] array = null;
        // if (loadColumnInfo.getSeperator().equalsIgnoreCase("0001")){
        //     byte  b[] = {'\u0001'};
        //     array= StringUtils.splitPreserveAllTokens(str,new String(b));
        // }else{
        //     array=StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        // }

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=8){
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }

        String provId=path.toString().split("=")[3].split("/")[0];
        String areaId=array[1];
        String mcc=array[2];

        lBSBuild.setProvId(provId);
        lBSBuild.setAreaId(areaId);
        lBSBuild.setMcc(mcc);

        String deviceNumberMd5=array[4].toUpperCase();
        String yyyyMMdd=array[6];
        String hhmmss=array[7];

        buff = lBSBuild.build().toByteArray();
        //rowkey: device_number_md5,yyyyMMdd,hhmmss  // 主叫年月日时分秒  // 主叫年月日时分秒
        StringBuilder keyValue=new StringBuilder();
        keyValue.append(deviceNumberMd5);
        keyValue.append(yyyyMMdd);
        keyValue.append(hhmmss);

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(column), buff);
        return put;
    }
}
