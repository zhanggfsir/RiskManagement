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

public class PJingxunInternetTaxiImpl implements ParseInterface{
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PJingXunTaxi.Builder pJingXunTaxi=Individualization.PJingXunTaxi.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pJingXunTaxi.setVisitCntSum((int) Double.parseDouble(array[1]));
        pJingXunTaxi.setPrivateCntSum((int) Double.parseDouble(array[2]));
        pJingXunTaxi.setLabel(array[3]);
        pJingXunTaxi.setProvId(array[4]);
        pJingXunTaxi.setAreaId(array[5]);


        buff = pJingXunTaxi.build().toByteArray();


        StringBuilder keyValue=new StringBuilder();
        String cert_no_md5=array[0];
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        //个性化的账期也是 手动输入
        //String accout=path.toString().split("/")[7];
        keyValue.append(cert_no_md5);
        account=account.substring(0,6);
        keyValue.append(account);

        //rowkey:device_number_md5,yyyyMM
        temp = Bytes.toBytes((short) (cert_no_md5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
