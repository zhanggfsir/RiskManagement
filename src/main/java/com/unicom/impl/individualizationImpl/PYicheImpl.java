package com.unicom.impl.individualizationImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Individualization;
import com.unicom.tools.InsertTable;
import com.unicom.utils.Encryption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
   hdfs://beh/user/lf_cp_serv/zb_serv.db/dm_v_m_use_yi_car/month_id=201907/prov_id=011
 */
public class PYicheImpl   implements ParseInterface {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);
    @Override
    public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Individualization.YiCar.Builder yiCar=Individualization.YiCar.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = null;
        if (loadColumnInfo.getSeperator().equalsIgnoreCase("0x01")){
            byte  b[] = {0x01};
            array= StringUtils.splitPreserveAllTokens(str,new String(b));
        }else{
            array=StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());
        }

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数{}与配置文件的{}不符--->{}",array.length, loadColumnInfo.getFieldNum(), str);
            int i=0;
            for(String ss:array){
                logger.info(i+"-->"+ss+"-->"+loadColumnInfo.getSeperator());
                i++;
            }
            return null;
        }
//        String path="hdfs://beh/user/lf_cp_serv/zb_serv.db/dm_v_m_use_yi_car/month_id=201907/prov_id=011";
//        String account=path.toString().split("=")[1].split("/")[0];
        String provId=path.toString().split("=")[2].split("/")[0];
        yiCar.setProvId(provId);
        yiCar.setBywxRank(array[1]);
        yiCar.setWzcxRank(array[2]);
        yiCar.setYcfwRank(array[3]);

        buff = yiCar.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();
        String deviceNumberMd5= Encryption.md5(String.valueOf(array[0])).toUpperCase();

        //String accout=path.toString().split("/")[7];
        // rowkey: cert_no_md5,yyyyMMdd
        keyValue.append(deviceNumberMd5);
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


/*

scan "p_yi_car",{COLUMNS =>["f:q"], LIMIT=>10}
建表
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.CreateTable 319 p_yi_car
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.CreateTable 419 p_yi_car

插入
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 319 p_yi_car q 201906
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.InsertTable 419 p_yi_car q 201906

查询
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.QueryTable 319 p_yi_car ui 262CA5841FCF3D2936646608F90AEE61 20190606

删除
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.DropTable  319 p_yi_car
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.DropTable  419 p_yi_car


重命名
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.RenameTable 319 p_yi_car p_yi_car

 */
