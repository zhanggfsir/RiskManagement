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

public class CvBatchQueryImpl implements QueryBatchInterface {
    private static Logger logger = LoggerFactory.getLogger(UiBatchQueryImpl.class);
    private static Table table;

    @Override
    public void put2Hdfs(FileSystem fs, LoadColumnInfo loadColumnInfo, HashSet<String> lineSet, String tableName, String account,
                         FSDataOutputStream hiveOutputStream, FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException {

        String zkName319 = "319";
        String familyName = loadColumnInfo.getFamilyName();
        String columnName = loadColumnInfo.getColumnName();
        GetConfigInfo getConfigInfo = new GetConfigInfo();
        ZkInfo zkInfo319 = getConfigInfo.getZkInfo(zkName319);
        HashMap<String, String> hdfsMap = new HashMap<>();

        for (String line : lineSet) {
            //1.得到 pathLine 。只从文件路径 得到省字段 。需要从路径获取字段 固定 | 分割。（可以写死了）
            String pathLine = line.substring(line.lastIndexOf("|") + 1);

            //2.得到 lineField 。（可以写死了）
            String fieldLine = line.substring(0, line.lastIndexOf("|"));
            String[] fieldArray = StringUtils.splitPreserveAllTokens(fieldLine, loadColumnInfo.getSeperator());

            //3. 将从HDFS中解析的数据，按照对应proto的顺序拼成字符串
            hdfsMap = getFieldFromHdfs(pathLine, fieldArray, hdfsMap, account);
        }
        //4.将1万条从HDFS中得到的数据写出
        HdfsUtil.saveHashMap2Hdfs(hiveOutputStream, hdfsMap);
        //5.去Hbase中查并写入文件
        getDataFromHbaseAndWrite(zkInfo319, tableName, familyName, columnName,
                hdfsMap, hiveOutputStream, errorOutputStream, successOutputStream);
        table.close();
    }

    public HashMap<String, String> getFieldFromHdfs(String linePath, String[] arrayField, HashMap<String, String> hdfsMap, String account) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(arrayField[0]);
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[1]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[2]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[3]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[4]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[5]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[6]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[7]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[8]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[9]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[10]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[11]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[12]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[13]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[14]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[15]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[16]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[17]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[18]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[19]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[20]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[21]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[22]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[23]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[24]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[25]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[26]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[27]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[28]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[29]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[30]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[31]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[32]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[33]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[34]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[35]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[36]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[37]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[38]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[39]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[40]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[41]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[42]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[43]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[44]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[45]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[46]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[47]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[48]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[49]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[50]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[51]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[52]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[53]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[54]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[55]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[56]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[57]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[58]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[59]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[60]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[61]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[62]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[63]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[64]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[65]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[66]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[67]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[68]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[69]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[70]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[71]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[72]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[73]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[74]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[75]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[76]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[77]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[78]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[79]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[80]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[81]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[82]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[83]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[84]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[85]));
        sb.append("|");
        sb.append((int) Double.parseDouble(arrayField[86]));
        //拼接得到k
        String userId = arrayField[0];
        String provId = linePath.split("=")[2].split("/")[0];
        account = account.substring(0, 6);
        String k = userId + "|" + provId + "|" + account;
        hdfsMap.put(k, sb.toString());
        return hdfsMap;
    }


    public void getDataFromHbaseAndWrite(ZkInfo zkInfo, String tableName, String familyName, String columnName,
                                         HashMap<String, String> hdfsMap, FSDataOutputStream hiveOutputStream, FSDataOutputStream errorOutputStream, FSDataOutputStream successOutputStream) throws IOException {
        List<Get> getList = new ArrayList<>();
        Iterator<String> it = hdfsMap.keySet().iterator();
        String account = null;
        while (it.hasNext()) {
            String rowkeyFromHdfsMap = it.next();
            String userId = StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[0];
            String provId = StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[1];
            account = StringUtils.splitPreserveAllTokens(rowkeyFromHdfsMap, "|")[2];

            //得到k  获得主键
            byte[] hash = Bytes.toBytes((short) (userId.hashCode() & 0x7FFF));
            account = account.substring(0, 6);
            String singleRowkey = userId + provId + account;
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
                    byte[] row = result.getRow();//得到rowkey  toString
                    StringBuilder sb = new StringBuilder();
                    for (Cell cell : list) {

                        Risk.VoiceHis voiceHis = Risk.VoiceHis.parseFrom(CellUtil.cloneValue(cell));
                        if (voiceHis.hasUserId()) sb.append(voiceHis.getUserId());
                        sb.append("|");
                        if (voiceHis.hasThisTollDura()) sb.append(voiceHis.getThisTotalDura());
                        sb.append("|");
                        if (voiceHis.hasThisInDura()) sb.append(voiceHis.getThisInDura());
                        sb.append("|");
                        if (voiceHis.hasThisOutDura()) sb.append(voiceHis.getThisOutDura());
                        sb.append("|");
                        if (voiceHis.hasThisLocalDura()) sb.append(voiceHis.getThisLocalDura());
                        sb.append("|");
                        if (voiceHis.hasThisTollDura()) sb.append(voiceHis.getThisTollDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamDura()) sb.append(voiceHis.getThisRoamDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvDura()) sb.append(voiceHis.getThisRoamProvDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvCallingDura()) sb.append(voiceHis.getThisRoamProvCallingDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvCalledDura()) sb.append(voiceHis.getThisRoamProvCalledDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamCounDura()) sb.append(voiceHis.getThisRoamCounDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamOutCounCallingD()) sb.append(voiceHis.getThisRoamOutCounCallingD());
                        sb.append("|");
                        if (voiceHis.hasThisRoamOutCounCalledDura()) sb.append(voiceHis.getThisRoamOutCounCalledDura());
                        sb.append("|");
                        if (voiceHis.hasThisRoamChangtuDura()) sb.append(voiceHis.getThisRoamChangtuDura());
                        sb.append("|");
                        if (voiceHis.hasThisTotalNums()) sb.append(voiceHis.getThisTotalNums());
                        sb.append("|");
                        if (voiceHis.hasThisInNums()) sb.append(voiceHis.getThisInNums());
                        sb.append("|");
                        if (voiceHis.hasThisOutNums()) sb.append(voiceHis.getThisOutNums());
                        sb.append("|");
                        if (voiceHis.hasThisLocalNums()) sb.append(voiceHis.getThisLocalNums());
                        sb.append("|");
                        if (voiceHis.hasThisTollNums()) sb.append(voiceHis.getThisTollNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamNums()) sb.append(voiceHis.getThisRoamNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvNums()) sb.append(voiceHis.getThisRoamProvNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvCallingNums()) sb.append(voiceHis.getThisRoamProvCallingNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamProvCalledNums()) sb.append(voiceHis.getThisRoamProvCalledNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamCounNums()) sb.append(voiceHis.getThisRoamCounNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamOutCounNums()) sb.append(voiceHis.getThisRoamOutCounNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamOutCounCallingN()) sb.append(voiceHis.getThisRoamOutCounCallingN());
                        sb.append("|");
                        if (voiceHis.hasThisRoamOutCounCalledNums()) sb.append(voiceHis.getThisRoamOutCounCalledNums());
                        sb.append("|");
                        if (voiceHis.hasThisRoamChangtuNums()) sb.append(voiceHis.getThisRoamChangtuNums());
                        sb.append("|");
                        if (voiceHis.hasLast3TotalDura()) sb.append(voiceHis.getLast3TotalNums());
                        sb.append("|");
                        if (voiceHis.hasLast3InDura()) sb.append(voiceHis.getLast3InNums());
                        sb.append("|");
                        if (voiceHis.hasLast3OutDura()) sb.append(voiceHis.getLast3OutDura());
                        sb.append("|");
                        if (voiceHis.hasLast3LocalDura()) sb.append(voiceHis.getLast3LocalDura());
                        sb.append("|");
                        if (voiceHis.hasLast3TollDura()) sb.append(voiceHis.getLast3TollDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamDura()) sb.append(voiceHis.getLast3RoamDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvDura()) sb.append(voiceHis.getLast3RoamProvDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvCallingDura()) sb.append(voiceHis.getLast3RoamProvCallingDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvCalledDura()) sb.append(voiceHis.getLast3RoamProvCalledDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamCounDura()) sb.append(voiceHis.getLast3RoamCounDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounDura()) sb.append(voiceHis.getLast3RoamOutCounDura());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounCallingD()) sb.append(voiceHis.getLast3RoamOutCounCallingD());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounCalledD()) sb.append(voiceHis.getLast3RoamOutCounCalledD());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamChangtuDura()) sb.append(voiceHis.getLast3RoamChangtuDura());
                        sb.append("|");
                        if (voiceHis.hasLast3TotalNums()) sb.append(voiceHis.getLast3TotalNums());
                        sb.append("|");
                        if (voiceHis.hasLast3InNums()) sb.append(voiceHis.getLast3InNums());
                        sb.append("|");
                        if (voiceHis.hasLast3OutNums()) sb.append(voiceHis.getLast3OutNums());
                        sb.append("|");
                        if (voiceHis.hasLast3LocalNums()) sb.append(voiceHis.getLast3LocalNums());
                        sb.append("|");
                        if (voiceHis.hasLast3TollNums()) sb.append(voiceHis.getLast3TollNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamNums()) sb.append(voiceHis.getLast3RoamNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvNums()) sb.append(voiceHis.getLast3RoamProvNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvCallingNums()) sb.append(voiceHis.getLast3RoamProvCallingNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamProvCalledNums()) sb.append(voiceHis.getLast3RoamProvCalledNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamCounNums()) sb.append(voiceHis.getLast3RoamCounNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounNums()) sb.append(voiceHis.getLast3RoamOutCounNums());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounCallingN()) sb.append(voiceHis.getLast3RoamOutCounCallingN());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamOutCounCalledN()) sb.append(voiceHis.getLast3RoamOutCounCalledN());
                        sb.append("|");
                        if (voiceHis.hasLast3RoamChangtuNums()) sb.append(voiceHis.getLast3RoamChangtuNums());
                        sb.append("|");
                        if (voiceHis.hasLast6TotalDura()) sb.append(voiceHis.getLast6TotalDura());
                        sb.append("|");
                        if (voiceHis.hasLast6InDura()) sb.append(voiceHis.getLast6InDura());
                        sb.append("|");
                        if (voiceHis.hasLast6OutDura()) sb.append(voiceHis.getLast6OutDura());
                        sb.append("|");
                        if (voiceHis.hasLast6LocalDura()) sb.append(voiceHis.getLast6LocalDura());
                        sb.append("|");
                        if (voiceHis.hasLast6TollDura()) sb.append(voiceHis.getLast6TollDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamDura()) sb.append(voiceHis.getLast6RoamDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvDura()) sb.append(voiceHis.getLast6RoamProvDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvCallingDura()) sb.append(voiceHis.getLast6RoamProvCallingDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvCalledDura()) sb.append(voiceHis.getLast6RoamProvCalledDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamCounDura()) sb.append(voiceHis.getLast6RoamCounDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounDura()) sb.append(voiceHis.getLast6RoamOutCounDura());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounCallingD()) sb.append(voiceHis.getLast6RoamOutCounCallingD());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounCalledD()) sb.append(voiceHis.getLast6RoamOutCounCalledD());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamChangtuDura()) sb.append(voiceHis.getLast6RoamChangtuDura());
                        sb.append("|");
                        if (voiceHis.hasLast6TotalNums()) sb.append(voiceHis.getLast6TotalNums());
                        sb.append("|");
                        if (voiceHis.hasLast6InNums()) sb.append(voiceHis.getLast6InNums());
                        sb.append("|");
                        if (voiceHis.hasLast6OutNums()) sb.append(voiceHis.getLast6OutNums());
                        sb.append("|");
                        if (voiceHis.hasLast6LocalNums()) sb.append(voiceHis.getLast6LocalNums());
                        sb.append("|");
                        if (voiceHis.hasLast6TollNums()) sb.append(voiceHis.getLast6TollNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamNums()) sb.append(voiceHis.getLast6RoamNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvNums()) sb.append(voiceHis.getLast6RoamProvNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvCallingNums()) sb.append(voiceHis.getLast6RoamProvCallingNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamProvCalledNums()) sb.append(voiceHis.getLast6RoamProvCalledNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamCounNums()) sb.append(voiceHis.getLast6RoamCounNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounNums()) sb.append(voiceHis.getLast6RoamOutCounNums());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounCallingN()) sb.append(voiceHis.getLast6RoamOutCounCallingN());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamOutCounCalledN()) sb.append(voiceHis.getLast6RoamOutCounCalledN());
                        sb.append("|");
                        if (voiceHis.hasLast6RoamChangtuNums()) sb.append(voiceHis.getLast6RoamChangtuNums());
                        sb.append("|");
                        if (voiceHis.hasLast6TotalDuraMax()) sb.append(voiceHis.getLast6TotalDuraMax());
                        sb.append("|");
                        if (voiceHis.hasLast6TotalDuraCv()) sb.append(voiceHis.getLast6TotalDuraCv());
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
