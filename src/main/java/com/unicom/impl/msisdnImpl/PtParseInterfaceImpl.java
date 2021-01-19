package com.unicom.impl.msisdnImpl;

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
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 319 dim_msisdn_sha256_md5 pt 20191016 >pt319.log &
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 419 dim_msisdn_sha256_md5 pt 20191016 >pt419.log &

 */
public class PtParseInterfaceImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

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
        md5Plaintext.setDeviceNumber(array[1]);
        md5Plaintext.setServType(array[2]);
        md5Plaintext.setProvId(array[3]);
        md5Plaintext.setAreaId(array[4]);
        md5Plaintext.setTurnOutDealer(array[5]);
        md5Plaintext.setEffectDate(array[6]);


        buff = md5Plaintext.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        String deviceNumberMd5=array[0].toUpperCase();
        keyValue.append(deviceNumberMd5);
        //手机号哈希，手机号和账期作为key
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
