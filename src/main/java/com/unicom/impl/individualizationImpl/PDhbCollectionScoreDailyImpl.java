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
电话邦-催收分
/user/lf_zh_pro/lf_dhb_pro/cu_csfx/csfx/result/20191213/part-00049.gz


15636931083|903|0|0|903|903|903|903|903|903|903|903|0|0|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|cuishou_db|2019-07-06
17663553450|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
18643703885|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
18597702248|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
13150555169|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
15680606715|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
13092247777|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
15697602782|901|0|0|901|901|901|901|901|901|901|901|0|0|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|901|cuishou_db|2019-07-06
13281575278|903|0|0|903|903|903|903|903|903|903|903|0|0|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|903|cuishou_db|2019-07-06
17692610269|0|0|0|91|91|0|0|0|0|0|0|0|0|91|91|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|cuishou_db|2019-07-06
 */
public class PDhbCollectionScoreDailyImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PDhbCollectionScoreDaily.Builder pDhbCollectionScoreDaily=Individualization.PDhbCollectionScoreDaily.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pDhbCollectionScoreDaily.setMainScore(array[1]);
        pDhbCollectionScoreDaily.setCsLatestPhoneTime(array[2]);
        pDhbCollectionScoreDaily.setCsFirstPhoneTime(array[3]);
        pDhbCollectionScoreDaily.setCsLatestPhoneDays(array[4]);
        pDhbCollectionScoreDaily.setCsFirstPhoneDays(array[5]);
        pDhbCollectionScoreDaily.setCsPhoneNumbers(array[6]);;

        pDhbCollectionScoreDaily.setCsTotalPhoneTimes(array[7]);
        pDhbCollectionScoreDaily.setCsCalledTimes(array[8]);
        pDhbCollectionScoreDaily.setCsPhoneTimesIn15S(array[9]);
        pDhbCollectionScoreDaily.setCsPhoneTimesBetween1530S(array[10]);
        pDhbCollectionScoreDaily.setCsPhoneTimesOver60S(array[11]);

        pDhbCollectionScoreDaily.setYscsLatestPhoneTime(array[12]);
        pDhbCollectionScoreDaily.setYscsFirstPhoneTime(array[13]);
        pDhbCollectionScoreDaily.setYscsLatestPhoneDays(array[14]);
        pDhbCollectionScoreDaily.setYscsFirstPhoneDays(array[15]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbers(array[16]);
        pDhbCollectionScoreDaily.setYscsTotalPhoneTimes(array[17]);

        pDhbCollectionScoreDaily.setYscsCalledTimes(array[18]);
        pDhbCollectionScoreDaily.setYscsPhoneTimesIn15S(array[19]);
        pDhbCollectionScoreDaily.setYscsPhoneTimesBetween1530S(array[20]);
        pDhbCollectionScoreDaily.setYscsPhoneTimesOver60S(array[21]);;

        pDhbCollectionScoreDaily.setCsPhoneNumbersIn7Days(array[22]);
        pDhbCollectionScoreDaily.setCsCalledTimesIn7Days(array[23]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbersIn7Days(array[24]);
        pDhbCollectionScoreDaily.setYscsCalledTimesIn7Days(array[25]);

        pDhbCollectionScoreDaily.setCsPhoneNumbersIn14Days(array[26]);
        pDhbCollectionScoreDaily.setCsCalledTimesIn14Days(array[27]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbersIn14Days(array[28]);;
        pDhbCollectionScoreDaily.setYscsCalledTimesIn14Days(array[29]);

        pDhbCollectionScoreDaily.setCsPhoneNumbersIn21Days(array[30]);
        pDhbCollectionScoreDaily.setCsCalledTimesIn21Days(array[31]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbersIn21Days(array[32]);
        pDhbCollectionScoreDaily.setYscsCalledTimesIn21Days(array[33]);

        pDhbCollectionScoreDaily.setCsPhoneNumbersIn30Days(array[34]);
        pDhbCollectionScoreDaily.setCsCalledTimesIn30Days(array[35]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbersIn30Days(array[36]);
        pDhbCollectionScoreDaily.setYscsCalledTimesIn30Days(array[37]);

        pDhbCollectionScoreDaily.setCsPhoneNumbersBetween3060Days(array[38]);
        pDhbCollectionScoreDaily.setCsCalledTimesBetween3060Days(array[39]);
        pDhbCollectionScoreDaily.setYscsPhoneNumbersBetween3060Days(array[40]);
        pDhbCollectionScoreDaily.setYscsCalledTimesBetween3060Days(array[41]);

        pDhbCollectionScoreDaily.setCuishouDb(array[42]);
        pDhbCollectionScoreDaily.setHandleTime(array[43]);

        buff = pDhbCollectionScoreDaily.build().toByteArray();
        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMD5=array[0];
        //String account=path.toString().split("/")[7];
        keyValue.append(deviceNumberMD5);
        keyValue.append(account);

        // rowkey: devicenumberMD5,yyyyMMdd
        temp = Bytes.toBytes((short) (deviceNumberMD5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}