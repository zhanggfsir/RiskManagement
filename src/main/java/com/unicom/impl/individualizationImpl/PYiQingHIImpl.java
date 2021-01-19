package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 区别
 * PYiQingHIImpl 霍旺做的 在湖北的人群
 * PYiQingImpl  庆力做的 湖北去往别的省的人群
 */
public class PYiQingHIImpl implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(PYiQingHIImpl.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.PYiQing.Builder yiQing=Individualization.PYiQing.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        //  由于要求 本次数据和之前放到相同的 表 列 ，此处对列进行特殊处理
        String columnName="q";
        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }
        String deviceNumberMd5=array[1];
        String dateDt=array[0];
        String provCode=array[5];
        yiQing.setDateDt(dateDt);
        yiQing.setDeviceNumberMd5(deviceNumberMd5);
        yiQing.setProvName(array[2]);;
        yiQing.setCityName(array[3]);
        yiQing.setCountyName(array[4]);
        yiQing.setProvCode(provCode);
        yiQing.setCityCode(array[6]);
        yiQing.setCountyCode(array[7]);
        yiQing.setFromProv(array[8]);
        yiQing.setFromCity(array[9]);
        yiQing.setAwayDt(array[10]);
        buff = yiQing.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        // rowkey: device_number_md5,yyyyMMdd,prov_code
        keyValue.append(deviceNumberMd5);
        keyValue.append(dateDt);//model_label
        keyValue.append(provCode);

        temp = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(columnName), buff);
        return put;
    }
}

