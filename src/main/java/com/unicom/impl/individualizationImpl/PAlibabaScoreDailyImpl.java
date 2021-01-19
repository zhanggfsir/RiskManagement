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
/user/lf_zh_pro/lf_ali_mobile_pro/output/month_id=201907/day_id=05/000173_0

0010100900149211|alibaba_secdm_mobile_risk_1|0.738|{"explains":{"是否上网":-1.348,"是否流量漫游":0.356,"使用状态":-0.305}}
0010211104272705499|alibaba_secdm_mobile_risk_1|0.65|{"explains":{"是否上网":-1.348,"使用状态":-0.509,"下行流量":-0.326}}
0010300149661|alibaba_secdm_mobile_risk_1|0.941|{"explains":{"是否集体实名认证":1.363,"是否在网":0.02,"是否4G":-0.006}}
0010300206928|alibaba_secdm_mobile_risk_1|0.941|{"explains":{"是否集体实名认证":1.363,"是否在网":0.02,"是否4G":-0.006}}
0010300228069|alibaba_secdm_mobile_risk_1|0.941|{"explains":{"是否集体实名认证":1.363,"是否在网":0.02,"是否4G":-0.006}}
0010BVPN004662|alibaba_secdm_mobile_risk_1|0.841|{"explains":{"是否个人实名认证":0.173,"是否在网":0.02,"是否4G":-0.006}}
0010DJ102215197|alibaba_secdm_mobile_risk_1|0.841|{"explains":{"是否个人实名认证":0.173,"是否在网":0.02,"是否4G":-0.006}}
0010DJ102238150|alibaba_secdm_mobile_risk_1|0.717|{"explains":{"是否上网":-1.348,"使用状态":-0.305,"是否个人实名认证":0.173}}
0010DJ102240043|alibaba_secdm_mobile_risk_1|0.841|{"explains":{"是否个人实名认证":0.173,"是否在网":0.02,"是否4G":-0.006}}
0010DJ102280645|alibaba_secdm_mobile_risk_1|0.841|{"explains":{"是否个人实名认证":0.173,"是否在网":0.02,"是否4G":-0.006}}
ls
 */
public class PAlibabaScoreDailyImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PAlibabaScoreDaily.Builder pAlibabaScoreDaily=Individualization.PAlibabaScoreDaily.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        pAlibabaScoreDaily.setModelLabel(array[1]);
        pAlibabaScoreDaily.setRiskScore(array[2]);
        pAlibabaScoreDaily.setExtraInfo(array[3]);
        buff = pAlibabaScoreDaily.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String deviceNumber=array[0];

        //String accout=path.toString().split("/")[7];
        // rowkey: device_number,model_label,yyyyMMdd
        keyValue.append(deviceNumber);
        keyValue.append(array[1]);//model_label
        keyValue.append(account);

        temp = Bytes.toBytes((short) (deviceNumber.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
}
