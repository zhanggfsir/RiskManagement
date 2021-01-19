package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Encryption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
拍拍信

 */

public class PPaipaixinImpl implements ParseInterface{
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {


        Individualization.PaiPaiXin.Builder pPaiPaiXin = Individualization.PaiPaiXin.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];
        String[] array = StringUtils.splitPreserveAllTokens(str, loadColumnInfo.getSeperator());

        if (array.length != loadColumnInfo.getFieldNum()) {
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pPaiPaiXin.setScore(array[2]);
        pPaiPaiXin.setMonthId(array[1]);
        buff = pPaiPaiXin.build().toByteArray();

        StringBuilder keyValue = new StringBuilder();
        String deviceNumberMd5=Encryption.md5(array[0]);
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumberMd5);
        keyValue.append(account);
        //test
        //11

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }

}

