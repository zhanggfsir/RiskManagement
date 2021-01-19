package com.unicom.queryImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.inter.QueryBatchInterface;
import com.unicom.risk.Risk;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.HbaseUtil;
import com.unicom.utils.HdfsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class CfBatchQueryImpl implements QueryBatchInterface {
    private static Logger logger = LoggerFactory.getLogger(UiBatchQueryImpl.class);
    private static Table table;
    @Override
    public void put2Hdfs(FileSystem fs, LoadColumnInfo loadColumnInfo, HashSet<String> lineSet, String tableName, String account,
                         FSDataOutputStream hiveOutputStream, FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException {

        String zkName319="319";
        String familyName=loadColumnInfo.getFamilyName();
        String columnName=loadColumnInfo.getColumnName();
        GetConfigInfo getConfigInfo = new GetConfigInfo();
        ZkInfo zkInfo319=getConfigInfo.getZkInfo(zkName319);
        HashMap<String,String> hdfsMap=new HashMap<>();

        for (String line:lineSet){
            //1.得到 pathLine 。只从文件路径 得到省字段 。需要从路径获取字段 固定 | 分割。（可以写死了）
            String pathLine=line.substring(line.lastIndexOf("|")+1);

            //2.得到 lineField 。（可以写死了）
            String fieldLine=line.substring(0,line.lastIndexOf("|"));
            String[] fieldArray = StringUtils.splitPreserveAllTokens(fieldLine, loadColumnInfo.getSeperator());

            //3. 将从HDFS中解析的数据，按照对应proto的顺序拼成字符串
            hdfsMap=getFieldFromHdfs(pathLine, fieldArray,hdfsMap,account);
        }
        //4.将1万条从HDFS中得到的数据写出
        HdfsUtil.saveHashMap2Hdfs(hiveOutputStream,hdfsMap);
        //5.去Hbase中查并写入文件
        getDataFromHbaseAndWrite(zkInfo319,tableName, familyName, columnName,
                hdfsMap,hiveOutputStream, errorOutputStream,successOutputStream);
        table.close();
    }

    public HashMap<String,String>  getFieldFromHdfs(String linePath, String[] arrayField,HashMap<String,String> hdfsMap,String account) throws IOException {
        StringBuilder sb=new StringBuilder();
        sb.append(arrayField[0]);
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[1]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[2]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[3]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[4]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[5]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[6]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[7]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[8]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[9]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[10]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[11]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[12]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[13]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[14]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[15]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[16]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[17]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[18]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[19]));
        sb.append("|");
        sb.append((int)Double.parseDouble(arrayField[20]));
        //拼接得到k
        System.out.println(sb.toString());
        String userId = arrayField[0];
        String provId = linePath.toString().split("=")[2].split("/")[0];
        account=account.substring(0,6);

        String  k=userId+"|"+provId+"|"+account;
        hdfsMap.put(k,sb.toString());
        return hdfsMap;
    }


    public void getDataFromHbaseAndWrite(ZkInfo zkInfo,String tableName,String familyName,String columnName,
                                         HashMap<String,String> hdfsMap,FSDataOutputStream hiveOutputStream,FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException {
        List<Get> getList=new ArrayList<>();
        Iterator<String> it = hdfsMap.keySet().iterator();
        String account = null;
        while (it.hasNext()) {
            String rowkeyFromHdfsMap = it.next();
            String userId = StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[0];
            String provId =StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[1];
            account= StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[2];

            //得到k  获得主键
            byte[] hash = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
            account=account.substring(0,6);
            String singleRowkey = userId +provId + account;
            byte[] rowkey = Bytes.add(hash, Bytes.toBytes(singleRowkey));

            Get get = new Get(rowkey);

            //String familyName=loadColumnInfo.getFamilyName();
            //String columnName=loadColumnInfo.getColumnName();
            //get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            // 当前默认值是false
            get.setCacheBlocks(zkInfo.isCache());
            get.setMaxVersions(zkInfo.getMaxVersion());
            getList.add(get);
        }

        table = HbaseUtil.getTable(zkInfo, tableName);
        System.out.println("tableName: "+tableName);
        Result[] resultArray = table.get(getList);

        for (Result result : resultArray) {
            if (!result.isEmpty()) {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(columnName));
                    byte[] row= result.getRow();//得到rowkey  toString
                    StringBuilder sb = new StringBuilder();
                    for (Cell cell : list) {

                        Risk.FluxHis fluxHis = Risk.FluxHis.parseFrom(CellUtil.cloneValue(cell));
                        if(fluxHis.hasUserId()) sb.append(fluxHis.getUserId());
                        sb.append("|");
                        if(fluxHis.hasThisFlux()) sb.append(fluxHis.getThisFlux());
                        sb.append("|");
                        if(fluxHis.hasThisLocalFlux()) sb.append(fluxHis.getThisLocalFlux());
                        sb.append("|");
                        if(fluxHis.hasThisRoamProvFlux()) sb.append(fluxHis.getThisRoamProvFlux());
                        sb.append("|");
                        if(fluxHis.hasThisRoamContFlux()) sb.append(fluxHis.getThisRoamContFlux());
                        sb.append("|");
                        if(fluxHis.hasThisRoamGatFlux()) sb.append(fluxHis.getThisRoamGatFlux());
                        sb.append("|");
                        if(fluxHis.hasThisRoamIntFlux()) sb.append(fluxHis.getThisRoamIntFlux());
                        sb.append("|");
                        if(fluxHis.hasLast3Flux()) sb.append(fluxHis.getLast3Flux());
                        sb.append("|");
                        if(fluxHis.hasLast3LocalFlux()) sb.append(fluxHis.getLast3LocalFlux());
                        sb.append("|");
                        if(fluxHis.hasLast3RoamProvFlux()) sb.append(fluxHis.getLast3RoamProvFlux());
                        sb.append("|");
                        if(fluxHis.hasLast3RoamContFlux()) sb.append(fluxHis.getLast3RoamContFlux());
                        sb.append("|");
                        if(fluxHis.hasLast3RoamGatFlux()) sb.append(fluxHis.getLast3RoamGatFlux());
                        sb.append("|");
                        if(fluxHis.hasLast3RoamIntFlux()) sb.append(fluxHis.getLast3RoamIntFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6Flux()) sb.append(fluxHis.getLast6Flux());
                        sb.append("|");
                        if(fluxHis.hasLast6LocalFlux()) sb.append(fluxHis.getLast6LocalFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6RoamProvFlux()) sb.append(fluxHis.getLast6RoamProvFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6RoamContFlux()) sb.append(fluxHis.getLast6RoamContFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6RoamGatFlux()) sb.append(fluxHis.getLast6RoamGatFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6RoamIntFlux()) sb.append(fluxHis.getLast6RoamIntFlux());
                        sb.append("|");
                        if(fluxHis.hasLast6FluxMax()) sb.append(fluxHis.hasLast6FluxMax());
                        sb.append("|");
                        if(fluxHis.hasLast6FluxCv()) sb.append(fluxHis.hasLast6FluxCv());
                        //如果有这一行记录，写入文件success 成功
                        //todo 将主键中的账期拆开
                        System.out.println(">>>"+sb.toString());
                        if(hdfsMap.containsValue(sb.toString())){
                            HdfsUtil.writeLine2Hdfs(successOutputStream, new String(row)+"|"+ sb.toString() + "_success");
                        }else{
                            //异常数据写入ERROR文件
                            HdfsUtil.writeLine2Hdfs(errorOutputStream, new String(row)+"|"+sb.toString() + "_error");
                        }
                    }
                }
            } else {
                logger.info("未查询到结果!");
            }

        }

    }
}
