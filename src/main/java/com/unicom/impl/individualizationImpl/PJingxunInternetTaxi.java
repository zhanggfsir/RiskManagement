package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PJingxunInternetTaxi  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PJingxunInternetTaxi.Builder pJingxunInternetTaxi=Individualization.PJingxunInternetTaxi.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = null;
        if (loadColumnInfo.getSeperator().equalsIgnoreCase("0x01")){
            byte  b[] = {0x01};
            array=StringUtils.splitPreserveAllTokens(str,new String(b));
        }else{
            array=StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        }

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数{}与配置文件的{}不符--->{}",array.length, loadColumnInfo.getFieldNum(), str);
            int i=0;
            for(String ss:array){
                logger.info(i+"-->"+ss+"-->"+loadColumnInfo.getSeperator());
                i++;
            }
            return null;
         }
        pJingxunInternetTaxi.setResult(array[3]);
        buff = pJingxunInternetTaxi.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String certNoMd5=array[0];

        // rowkey: cert_no_md5,yyyyMMdd
        keyValue.append(certNoMd5);
        keyValue.append(account);

        temp = Bytes.toBytes((short) (certNoMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}

