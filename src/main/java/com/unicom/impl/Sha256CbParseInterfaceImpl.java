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
 * zba_dwa.dwa_v_d_cus_al_rns_encap
 * zba_dwa.dwa_v_d_cus_cb_rns_encap
 *
 *  UBD_B_DWA.DWA_D_CUS_AL_CUST_ENCAP
 * // 表: user_daily_msisdn
 * // 列: f:sha256
 * // rowkey: device_number_sha256,yyyyMMdd
 * message RnsEncap {
 *     optional string cust_name_sha256 = 1 ; // 姓名加密_sha256
 *     optional string cert_no_sha256   = 2 ; // 证件号码加密_sha256
 *     optional string cert_type        = 3 ; // 证件类型
 * }
 */
public class Sha256CbParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.RnsEncap.Builder rnsEncapBuild=Risk.RnsEncap.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }if(account.length()!=8){
            logger.error("输入账期的格式为 yyyyMMdd,当前输入账期为{},不符合规范", account);
            return null;
        }


        String provId=path.toString().split("=")[3].split("/")[0];
        rnsEncapBuild.setCustNameSha256(array[6]);
        rnsEncapBuild.setCertNoSha256(array[7]);
        rnsEncapBuild.setCertType(array[5]);
        buff = rnsEncapBuild.build().toByteArray();

        //rowkey: device_number_sha256,yyyyMMdd
        StringBuilder keyValue=new StringBuilder();
        String deviceNumber=array[0];

        Md5Util md5Util=new Md5Util();
        String deviceNumberMd5=md5Util.md5(deviceNumber).toUpperCase();
        keyValue.append(deviceNumberMd5);
        keyValue.append(account);
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}