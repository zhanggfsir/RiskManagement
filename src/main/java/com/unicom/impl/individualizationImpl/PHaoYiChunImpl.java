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
/*
/user/lf_zh_pro/lf_ylxyf_pro/resultdata/month_id=201906/000016_0
*/
// 手机号|省|账期|策略
public class PHaoYiChunImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.HaoYiChun.Builder haoYiChun=Individualization.HaoYiChun.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        haoYiChun.setProvId(array[1]);
        haoYiChun.setMonthId(array[2]);
        haoYiChun.setStrategy(array[3]);

        buff = haoYiChun.build().toByteArray();

        // rowkey: device_number_md5,yyyyMM
        StringBuilder keyValue=new StringBuilder();

        Md5Util md5Util=new Md5Util();
        String deviceNumberMd5=md5Util.md5(array[0]);
        keyValue.append(deviceNumberMd5);

        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
