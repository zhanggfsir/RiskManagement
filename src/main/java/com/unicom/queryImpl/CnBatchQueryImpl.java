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

public class CnBatchQueryImpl implements QueryBatchInterface{
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
        sb.append(Integer.parseInt(arrayField[1]));
        sb.append("|");
        sb.append(Integer.parseInt(arrayField[2]));

        //拼接得到k
        String deviceNumberMd5 = arrayField[0].toUpperCase();
        account=account.substring(0,6);
        String  k=deviceNumberMd5+"|"+account;
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
            String deviceNumberMd5 = StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[0].toUpperCase();
            account= StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[1];

            //得到k  获得主键
            byte[] hash = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
            account=account.substring(0,6);
            String singleRowkey = deviceNumberMd5 + account;
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
        Result[] resultArray = table.get(getList);

        for (Result result : resultArray) {
            if (!result.isEmpty()) {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(columnName));
                    byte[] row= result.getRow();//得到rowkey  toString
                    StringBuilder sb = new StringBuilder();
                    for (Cell cell : list) {

                        Risk.ConcatNum concatNum = Risk.ConcatNum.parseFrom(CellUtil.cloneValue(cell));
                        if(concatNum.hasContactNum1()) sb.append(concatNum.getContactNum1());
                        sb.append("|");
                        if(concatNum.hasContactNum2()) sb.append(concatNum.getContactNum2());
                        //如果有这一行记录，写入文件success 成功
                        //todo 将主键中的账期拆开
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
