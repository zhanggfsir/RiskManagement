package com.unicom.impl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.inter.ParseInterface;
import com.unicom.risk.Risk;
import com.unicom.tools.InsertTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimCellParseInterfaceImpl implements ParseInterface

    {
        private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

        @Override
        public Put getPut(String str, LoadColumnInfo loadColumnInfo, Path path, String account) {
        Risk.DimCell.Builder dimCell=Risk.DimCell.newBuilder();
        byte[] buff = new byte[0];
        byte[] temp = new byte[0];

        String[] array = StringUtils.splitPreserveAllTokens(str,loadColumnInfo.getSeperator());

        if(array.length!=loadColumnInfo.getFieldNum()){
            logger.error("读取的如下内容字段个数与配置文件的{}不符--->{}", loadColumnInfo.getFieldNum(), str);
            return null;
        }

            dimCell.setProvId(array[0]); //array[]
            dimCell.setAreaId(array[2]);
            dimCell.setDistrictId(array[4]);
            try{dimCell.setLat(Double.parseDouble(array[6])); } catch (NumberFormatException e) { }
            try{dimCell.setLon(Double.parseDouble(array[7])) ;} catch (NumberFormatException e) { }
            try{dimCell.setNetType(Integer.parseInt(array[12])) ;} catch (NumberFormatException e) { }
            dimCell.setProvDesc(array[1]);
            dimCell.setAreaDesc(array[3]);
            dimCell.setDistrictDesc(array[5]);


        buff = dimCell.build().toByteArray();

        StringBuilder keyValue=new StringBuilder();

        String lac=array[8];
        String ci=array[9];

        keyValue.append(lac);
        keyValue.append(ci);
        // rowkey: lac,ci
        temp = Bytes.toBytes((short) (lac.hashCode() & 0x7FFF));
        temp = Bytes.add(temp, Bytes.toBytes(keyValue.toString()));
        Put put = new Put(temp);

        put.setDurability(Durability.SKIP_WAL);
        put.addImmutable(Bytes.toBytes(loadColumnInfo.getFamilyName()), Bytes.toBytes(loadColumnInfo.getColumnName()), buff);
        return put;
    }
    }

