package com.unicom.queryImpl;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.inter.QueryBatchInterface;
import com.unicom.risk.Risk;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.CommonUtil;
import com.unicom.utils.HbaseUtil;
import com.unicom.utils.HdfsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AfxBatchQueryImpl implements QueryBatchInterface {
    private static Logger logger = LoggerFactory.getLogger(AfxBatchQueryImpl.class);
    private static Table table;
    @Override
    public void put2Hdfs(FileSystem fs, LoadColumnInfo loadColumnInfo, HashSet<String> lineSet, String tableName, String account,
                         FSDataOutputStream hiveOutputStream,FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException {

        String zkName319="319";
        String familyName=loadColumnInfo.getFamilyName();
        String columnName=loadColumnInfo.getColumnName();
        GetConfigInfo getConfigInfo = new GetConfigInfo();
        ZkInfo zkInfo319=getConfigInfo.getZkInfo(zkName319);
        HashMap<String,String> hdfsMap=new HashMap<>();
        System.out.println("分隔符："+loadColumnInfo.getSeperator());
        for (String line:lineSet){
//            System.out.println(line);
            //1.得到 pathLine 。只从文件路径 得到省字段 。需要从路径获取字段 固定 | 分割。（可以写死了）
            String pathLine=line.substring(line.lastIndexOf("|")+1);

            //2.得到 lineField 。（可以写死了）
            byte  b[] = {0x01};
            String fieldLine=line.substring(0,line.lastIndexOf("|"));
            String[] fieldArray = StringUtils.splitPreserveAllTokens(fieldLine,new String(b));
//            System.out.println("fieldArray数组长度："+fieldArray.length);
//            System.out.println("第16个字段是："+fieldArray[15]);
//            System.out.println();
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
        sb.append(arrayField[1]);
        sb.append("|");
        sb.append(arrayField[3]);
        sb.append("|");
        sb.append(arrayField[4]);
        sb.append("|");
        sb.append(arrayField[5]);
        sb.append("|");
        sb.append(arrayField[6]);
        sb.append("|");
        sb.append(arrayField[7]);
        sb.append("|");
        sb.append(arrayField[8]);
        sb.append("|");
        sb.append(arrayField[9]);
        sb.append("|");
        sb.append(arrayField[10]);
        sb.append("|");
        sb.append(arrayField[11]);
        sb.append("|");
        sb.append(arrayField[12]);
        sb.append("|");
        sb.append(arrayField[13]);
        sb.append("|");
        sb.append(arrayField[14]);
        sb.append("|");
        sb.append(arrayField[16]);
        sb.append("|");
        sb.append(arrayField[17]);
        sb.append("|");
        sb.append(arrayField[18]);
        sb.append("|");
        sb.append(arrayField[19]);
        // 需要解析
        System.out.println(sb.toString());
        String deviceNumberMd5 = arrayField[15].toUpperCase();
        String  k=deviceNumberMd5+"|"+account;
//        System.out.println("deviceNumberMd5 "+deviceNumberMd5);
//        System.out.println("account "+account);
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
            String singleRowkey = deviceNumberMd5 + account;
//            System.out.println("rowkey >>> "+singleRowkey);
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
                        Risk.AfxUserInfo afxUserInfo = Risk.AfxUserInfo.parseFrom(CellUtil.cloneValue(cell));
                        if (afxUserInfo.hasAreaId()) sb.append(afxUserInfo.getAreaId());
                        sb.append("|");
                        if (afxUserInfo.hasUserId()) sb.append(afxUserInfo.getUserId());
                        sb.append("|");
                        if (afxUserInfo.hasCustId()) sb.append(afxUserInfo.getCustId());
                        sb.append("|");
                        if (afxUserInfo.hasServiceType()) sb.append(afxUserInfo.getServiceType());
                        sb.append("|");
                        if (afxUserInfo.hasPayMode()) sb.append(afxUserInfo.getPayMode());
                        sb.append("|");
                        if (afxUserInfo.hasProductId()) sb.append(afxUserInfo.getProductId());
                        sb.append("|");
                        if (afxUserInfo.hasProductMode()) sb.append(afxUserInfo.getProductMode());
                        sb.append("|");
                        if (afxUserInfo.hasInnetDate()) sb.append(afxUserInfo.getInnetDate());
                        sb.append("|");
                        if (afxUserInfo.hasCloseDate()) sb.append(afxUserInfo.getCloseDate());
                        sb.append("|");
                        if (afxUserInfo.hasInnetMonths()) sb.append(afxUserInfo.getInnetMonths());
                        sb.append("|");
                        if (afxUserInfo.hasIsInnet()) sb.append(afxUserInfo.getIsInnet());
                        sb.append("|");
                        if (afxUserInfo.hasIsStat()) sb.append(afxUserInfo.getIsStat());
                        sb.append("|");
                        if (afxUserInfo.hasUserIdEn()) sb.append(afxUserInfo.getUserIdEn());
                        sb.append("|");
                        if (afxUserInfo.hasUserStatus()) sb.append(afxUserInfo.getUserStatus());
                        sb.append("|");
                        if (afxUserInfo.hasStopType()) sb.append(afxUserInfo.getStopType());
                        sb.append("|");
                        if (afxUserInfo.hasLastStopDate()) sb.append(afxUserInfo.getLastStopDate());
                        sb.append("|");
                        if (afxUserInfo.hasChannelId()) sb.append(afxUserInfo.getChannelId());

                        //如果有这一行记录，写入文件success 成功
                        //todo 将主键中的账期拆开
                        System.out.println(">>>"+sb.toString());
                        if(hdfsMap.containsValue(sb.toString())){
//                            System.out.println("contains");
                            HdfsUtil.writeLine2Hdfs(successOutputStream, new String(row)+"|"+ sb.toString() + "_success");
                        }else{
                            //异常数据写入ERROR文件
//                            System.out.println("not contains");
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
