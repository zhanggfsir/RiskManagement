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

/*
1.加工出全量表
sh -x p_dim_inc_msisdn_info.sh 20190828 release

java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 319 dim_msisdn_sha256_md5 pt 20191014

nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 319 dim_msisdn_sha256_md5 pt 20191018 >319pt.log &
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 419 dim_msisdn_sha256_md5 pt 20191018 >419pt.log &

 */
public class PtOneParseInterfaceImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(PtOneParseInterfaceImpl.class);

        @Override
        public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
            Risk.Md5Plaintext.Builder md5Plaintext=Risk.Md5Plaintext.newBuilder();
            byte[] buff = new byte[0];
            byte[] temp = new byte[0];

            String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

            if(array.length!=loadColumnInfo.getFieldNum()){
                logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
                return null;
            }
            String deviceNumberMd5=array[0];
            md5Plaintext.setDeviceNumber(array[1]);
            md5Plaintext.setServType(array[2]);
            md5Plaintext.setProvId(array[3]);
            md5Plaintext.setAreaId(array[4]);
            md5Plaintext.setTurnOutDealer(array[5]);
            md5Plaintext.setEffectDate(array[6]);


            buff = md5Plaintext.build().toByteArray();

            // rowkey: device_no // 号段
            temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
            temp = Bytes.add(temp, Bytes.toBytes(deviceNumberMd5));
            Put put = new Put(temp);

            put.setDurability(Durability.SKIP_WAL);
            put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
            return put;

    }
}
