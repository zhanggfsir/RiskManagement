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
度小满
/user/lf_zh/lf_dxm_pro/modle_result/month_id=201905/000016_0

C27B99519FBCAF4DA4ACF9A63EFD8886|B15984895|2019-05-01|0.25542688|081
FAD39ED78A889DD2ABC8A56823D02321|A8118102683671462|2019-05-01|0.15197387|081
6E2C769FD0D2C9294AC6C6B309E7CE59|A8118061264204407|2019-05-01|0.3543353|081
7614F7E96B95304B80D7B0497411443C|A8115052935201833|2019-05-01|0.295919|081
BC17BCCE7CAED77B1D6AE36B41A37C2C|A8117022522540386|2019-05-01|0.22928965|081
E319439953B0C96210125C19E4434A8A|A8118120493486858|2019-05-01|0.3183494|081
F7C02E22DA8A315FD3E63558E008059D|A8118080271044793|2019-05-01|0.55653757|081
E1F2BAA45A6B6D1033BE3D35C4D4333D|B19823500|2019-05-01|0.24166666|081
261AD46D92C0DC8ED97911D4BCB2B921|A8119022857183596|2019-05-01|0.3971749|081
5BED5054A9B6D576088D0AE3ED7D9175|A8117110890277656|2019-05-01|0.42927724|081

 */
public class PDuxiaomanMonthlyImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PDuxiaomanMonthly.Builder pDuxiaomanMonthly=Individualization.PDuxiaomanMonthly.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pDuxiaomanMonthly.setUserId(array[1]);
        pDuxiaomanMonthly.setMonthId(array[2]);
        pDuxiaomanMonthly.setPredScoreA(array[3]);
        pDuxiaomanMonthly.setProvId(array[4]);

        buff = pDuxiaomanMonthly.build().toByteArray();


        StringBuilder keyValue=new StringBuilder();
        String deviceNumber=array[0];
        //个性化的账期也是 手动输入
        //String accout=path.toString().split("/")[7];
        keyValue.append(deviceNumber);
        //月表截取前6位，当输入8位账期时，程序仍然鲁棒棒的
        account=account.substring(0,6);
        keyValue.append(account);

        //rowkey: device_number_md5,yyyyMM
        temp = Bytes.toBytes((short) (deviceNumber.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
