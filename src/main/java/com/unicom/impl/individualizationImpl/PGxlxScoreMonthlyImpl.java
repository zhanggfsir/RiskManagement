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
国信利信（优易） 
/user/lf_zh_pro/lf_gxyy_pro/month_id=201905/201905.txt

558EA49458DBED4202BE7810C1EF1A4D|24243704|201905
24FA5D4AFC2974AD13F4F85F8066F30B|16836104|201905
8FE52BE9B91F610969FA147EA2C0EB85|13476704|201905
23A22093A081026F343282D29C137464|13495004|201905
2FFEFACB781467D0EBCEB3D188C3EFB5|22054804|201905
DC6F8BB7C136F61958371D86BA94A643|24253904|201905
1E0C0D55B69483A8F8230A5BECE25FE2|24235404|201905
B6325227CC99C69BC8A19EDD20EBBC52|24235404|201905
DD351A404C1E02E31262F9F845ABD7FA|24235404|201905
D3B3E7D37C88475D4ABE63702BD3372B|16824204|201905

 */
public class PGxlxScoreMonthlyImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PGxlxScoreMonthly.Builder pGxlxScoreMonthly=Individualization.PGxlxScoreMonthly.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pGxlxScoreMonthly.setScore(array[1]);
        pGxlxScoreMonthly.setMonthId(array[2]);


        buff = pGxlxScoreMonthly.build().toByteArray();


        StringBuilder keyValue=new StringBuilder();
        String deviceNumber=array[0];
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        //个性化的账期也是 手动输入
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumber);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);

        //rowkey:device_number_md5,yyyyMM
        temp = Bytes.toBytes((short) (deviceNumber.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
