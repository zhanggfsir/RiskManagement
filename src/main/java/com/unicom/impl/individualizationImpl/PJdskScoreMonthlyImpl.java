package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Md5Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PJdskScoreMonthlyImpl implements ParseInterface{
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PJdskScoreMonthly.Builder pJdskScoreMonthly=Individualization.PJdskScoreMonthly.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pJdskScoreMonthly.setScore(array[1]);
        pJdskScoreMonthly.setStability(array[2]);
        pJdskScoreMonthly.setNetTime(array[3]);
        pJdskScoreMonthly.setConsumeIndex(array[4]);
        pJdskScoreMonthly.setDataIndex(array[5]);
        pJdskScoreMonthly.setCallIndex(array[6]);
        pJdskScoreMonthly.setOverStopIndex(array[7]);
        pJdskScoreMonthly.setLongIndex(array[8]);

        buff = pJdskScoreMonthly.build().toByteArray();


        StringBuilder keyValue=new StringBuilder();
        Md5Util md5Util=new Md5Util();
        String deviceNumberMd5=md5Util.md5(array[0]);
        //个性化的账期也是 手动输入
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumberMd5);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);

        //rowkey:device_number_md5,yyyyMM
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
