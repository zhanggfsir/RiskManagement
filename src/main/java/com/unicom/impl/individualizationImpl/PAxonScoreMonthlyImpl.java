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
/*
安迅信用分
/user/lf_zh_pro/lf_ax_pro/output/month_id=201905/score_result_v62_201905.txt

2D7D51AA448C9813FF5353227369AF21|5549|4628|085|106.0
CE318F6DC129816D109BEB840EBDA6EE|5549|4797|085|67.0
9069F861C8268A9FAF45F20E575018CB|5491|4754|085|32.0
D702B95AED916D087C2753859643DAA2|5549|4797|085|70.0
C399D10A915607C32CF5276694F46BB7|5549|5116|085|57.0
9C4B736E3CE5B9F177B73A763194D91B|5491|\N|085|29.0
E81977BFA81FB633488E37A94AFF4414|5491|\N|085|27.0
97378A14F8E2D9D773648A226A6492DF|5549|4634|085|34.0
F185C17928762CA357932020B60FF424|5491|\N|085|30.0
86DB5562ED33240D2CD5C8D8B07D7648|5549|4738|085|120.0

 */
public class PAxonScoreMonthlyImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PAxonScoreMonthly.Builder pAxonScoreMonthly=Individualization.PAxonScoreMonthly.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pAxonScoreMonthly.setScore(array[1]);
        pAxonScoreMonthly.setConfidence(array[2]);
        pAxonScoreMonthly.setProvId(array[3]);
        pAxonScoreMonthly.setInnetDate(array[4]);


        buff = pAxonScoreMonthly.build().toByteArray();


        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMd5=array[0];
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumberMd5);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);

        // rowkey: device_number_md5,yyyyMM
        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}

