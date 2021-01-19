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
电话邦-读秒分 电话邦信用分
/user/lf_zh_pro/lf_dhb_pro/cu_csfx/calllog7/result/20191213/part-00049.gz

13014041658|218|0.4|1|84|0|0.01|5|595|3749|585|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
17677047819|13|0.0|0|70|5|0.08|34|436|469|1367|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
13147085921|131|0.24|1|81|1|0.0|8|626|2344|575|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
18623342817|431|0.47|3|89|0|0.01|2|675|4737|582|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
15635227918|2505|0.43|13|89|0|0.05|0|542|4513|603|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
13277196616|8|1.0|0|88|73|0.0|73|569|8892|5032|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
18567390536|193|0.29|1|88|0|0.0|9|564|2813|575|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
15616294300|311|0.35|2|89|0|0.01|1|639|3307|581|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
15546063|2|1.0|0|47|46|0.0|46|569|8892|4197|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
13219321667|67|0.51|1|89|39|0.0|39|526|5150|4290|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|2019-07-06
text: Unable to write to output stream.

 */
public class PDhbSecondScoreDailyImpl  implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PDhbSecondScoreDaily.Builder  pDhbSecondScoreDaily=Individualization.PDhbSecondScoreDaily.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pDhbSecondScoreDaily.setC1(array[1]);
        pDhbSecondScoreDaily.setC2(array[2]);
        pDhbSecondScoreDaily.setC3(array[3]);
        pDhbSecondScoreDaily.setC4(array[4]);
        pDhbSecondScoreDaily.setC5(array[5]);
        pDhbSecondScoreDaily.setC6(array[6]);
        pDhbSecondScoreDaily.setC7(array[7]);
        pDhbSecondScoreDaily.setC8(array[8]);
        pDhbSecondScoreDaily.setC9(array[9]);
        pDhbSecondScoreDaily.setC10(array[10]);
        pDhbSecondScoreDaily.setC11(array[11]);
        pDhbSecondScoreDaily.setC12(array[12]);
        pDhbSecondScoreDaily.setC13(array[13]);
        pDhbSecondScoreDaily.setC14(array[14]);
        pDhbSecondScoreDaily.setC15(array[15]);
        pDhbSecondScoreDaily.setC16(array[16]);
        pDhbSecondScoreDaily.setC17(array[17]);
        pDhbSecondScoreDaily.setC18(array[18]);
        pDhbSecondScoreDaily.setC19(array[19]);
        pDhbSecondScoreDaily.setC20(array[20]);
        pDhbSecondScoreDaily.setC21(array[21]);
        pDhbSecondScoreDaily.setC22(array[22]);
        pDhbSecondScoreDaily.setC23(array[23]);
        pDhbSecondScoreDaily.setC24(array[24]);
        pDhbSecondScoreDaily.setC25(array[25]);
        pDhbSecondScoreDaily.setC26(array[26]);
        pDhbSecondScoreDaily.setC27(array[27]);
        pDhbSecondScoreDaily.setC28(array[28]);
        pDhbSecondScoreDaily.setC29(array[29]);
        pDhbSecondScoreDaily.setC30(array[30]);
        pDhbSecondScoreDaily.setC31(array[31]);

        buff = pDhbSecondScoreDaily.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMD5=array[0];
        //个性化的账期也是 手动输入
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumberMD5);
        keyValue.append(account);

        temp = Bytes.toBytes((short) (deviceNumberMD5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
