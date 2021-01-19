package test;

import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.risk.Risk;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.HbaseUtil;
import com.unicom.utils.HdfsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/*
/user/lf_by_pro/zba_dwa.db/dwa_v_d_cus_al_user_info/part_id=201909/day_id=24/prov_id=010/ HashSet
nohup java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.tools.QueryBatch user_daily_msisdn ui 20190924 >query.log &
scan 'user_daily_msisdn',{COLUMNS=>['f:ui'],LIMIT=>10}
 */
//implements QueryBatchInterface
public class UiBatchQueryImplv1  {
    private static Logger logger = LoggerFactory.getLogger(UiBatchQueryImplv1.class);
    private static Table table;

//    @Override
    public void put2Hdfs(FileSystem fs, LoadColumnInfo loadColumnInfo, HashSet<String> lineSet, String tableName, String account,
                         FSDataOutputStream dataOutputStream,FSDataOutputStream distinctionOutputStream, FSDataOutputStream successOutputStream) throws IOException {

        String zkName319="319";
        String zkName419="419";
        GetConfigInfo getConfigInfo = new GetConfigInfo();
        ZkInfo zkInfo319=getConfigInfo.getZkInfo(zkName319);
        ZkInfo zkInfo419=getConfigInfo.getZkInfo(zkName419);
        HashMap<String,String> hdfsMap=new HashMap<>();
        HashMap<String,String> hbaseMap=new HashMap<>();
//        String k;

        List<Get> getList=new ArrayList<>();
        //TODO 传入的账期可能是6位的 此处 换成8位的 ！ 注意账期的长度
            for (String line:lineSet){
                //1.得到 pathLine 。只从文件路径 得到省 等。需要从路径获取字段 固定 | 分割。（可以写死了）
                String pathLine=line.substring(line.lastIndexOf("|")+1);
//                logger.info("linePath -->"+pathLine);

                //2.得到 lineField 。（可以写死了）
                String fieldLine=line.substring(0,line.lastIndexOf("|"));
                String[] fieldArray = StringUtils.splitPreserveAllTokens(fieldLine, loadColumnInfo.getSeperator());

                //3. 将从HDFS中解析的数据，按照对应proto的顺序拼成字符串写出 ,getFieldFromHdfsAndWrite
                hdfsMap=getFieldFromHdfsAndWrite(pathLine, fieldArray,hdfsMap,account);

                //4.去Hbase中查并写入文件
                //得到k  获得主键
                String deviceNumberMd5 = fieldArray[17].toUpperCase();
                byte[] hash = Bytes.toBytes((short) (deviceNumberMd5.hashCode() & 0x7FFF));
                byte[] rowkey = Bytes.add(hash, Bytes.toBytes(deviceNumberMd5 + account));

//                k=deviceNumberMd5 + account;

                Get get = new Get(rowkey);


//                get.addFamily();
                String familyName=loadColumnInfo.getFamilyName();
                String columnName=loadColumnInfo.getColumnName();
                get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
                // 当前默认值是false
                get.setCacheBlocks(zkInfo319.isCache());
                get.setMaxVersions(zkInfo319.getMaxVersion());
                getList.add(get);
            }

            hbaseMap=getDataFromHbaseAndWrite(zkInfo319,zkInfo419,loadColumnInfo,tableName,getList,hbaseMap);

            Set<Map.Entry<String,String>> entrySet=hdfsMap.entrySet();
            for (Map.Entry<String,String> entry:entrySet){
                String k =entry.getKey();
                String v =entry.getValue();
                //写入文件
                if (!hbaseMap.get(k).equalsIgnoreCase(v)){
                    String hdfsStr=k+v;
                    String hbaseStr=k+hbaseMap.get(k);
                    HdfsUtil.writeLine2Hdfs(distinctionOutputStream,"----------warning----------");
                    HdfsUtil.writeLine2Hdfs(distinctionOutputStream,"hdfs 的数据为-->"+hdfsStr);
                    HdfsUtil.writeLine2Hdfs(distinctionOutputStream,"hbase的数据为==>"+hbaseStr);
                }else {
                    String hdfsStr=k+v;

                    String hbaseStr=k+hbaseMap.get(k);
                    HdfsUtil.writeLine2Hdfs(successOutputStream,"----------right----------");
                    HdfsUtil.writeLine2Hdfs(successOutputStream,"hdfs 的数据为-->"+hdfsStr);
                    HdfsUtil.writeLine2Hdfs(successOutputStream,"hbase的数据为==>"+hbaseStr);
                }

            }

            table.close();
    }


    public HashMap<String,String>  getFieldFromHdfsAndWrite(String linePath, String[] arrayField,HashMap<String,String> hdfsMap,String account) throws IOException {
        StringBuilder sb=new StringBuilder();
        String provId=linePath.split("=")[3].split("/")[0];
        sb.append(arrayField[1]);
        sb.append("|");
        sb.append(arrayField[2]);
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
        sb.append(arrayField[15]);
        sb.append("|");
        sb.append(arrayField[16]);
        sb.append("|");
        sb.append(arrayField[18]);
        sb.append("|");
        sb.append(arrayField[20]);
        sb.append("|");
        sb.append(arrayField[21]);
        sb.append("|");
        sb.append(arrayField[24]);
        sb.append("|");
        sb.append(arrayField[25]);
        sb.append("|");
        // 需要解析
        sb.append(provId);
        //拼接得到k
        String deviceNumberMd5 = arrayField[17].toUpperCase();
        String  k=deviceNumberMd5+account;
        hdfsMap.put(k,sb.toString());
        //HdfsUtil.writeLine2Hdfs(saveFromHdfs2HdfsfileOutputStream,sb.toString());
        return hdfsMap;
    }

    /**
     * 将从HBASE中查得的数据，分别写数据到 319 419
     */
    public  HashMap<String,String>  getDataFromHbaseAndWrite(ZkInfo zkInfo319,ZkInfo zkInfo419,
                                                 LoadColumnInfo loadColumnInfo,String tableName,List<Get> getList,HashMap<String,String> hbaseMap) throws IOException {

        String familyName=loadColumnInfo.getFamilyName();
        String columnName=loadColumnInfo.getColumnName();

        return getDataFromHbaseAndWrite( zkInfo319,tableName,familyName,columnName, getList, hbaseMap);
//        getDataFromHbaseAndWrite( zkInfo419,tableName,familyName,columnName, getList, fileOutputStreamC, hbaseMap);
//        getDataFromHbaseAndWrite2( zkInfo419,tableName,familyName,columnName, getList, fileOutputStreamC);
    }


    private static HashMap<String,String> getDataFromHbaseAndWrite(ZkInfo zkInfo,String tableName,String familyName,String columnName,
                                       List<Get> getList,HashMap<String,String> hbaseMap) throws IOException {
        table = HbaseUtil.getTable(zkInfo, tableName);
        Result[] resultArray = table.get(getList);

        for (Result result : resultArray) {

            if (!result.isEmpty()) {
                int i;
                for (String family : familyName.split(",")) {
                    List<Cell> list = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(columnName));
                    StringBuilder sb = new StringBuilder();
                    for (Cell cell : list) {
                        // todo 核验此处是否是 k
                        String k = String.valueOf(cell.getRowArray());
                        String str = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.println("--------str----------"+str);
                        System.out.println("--------k----------"+k);
                        Risk.AlUserInfo alUserInfo = Risk.AlUserInfo.parseFrom(CellUtil.cloneValue(cell));
//                            alUserInfo.
                        sb.append(alUserInfo.getAreaId());
                        sb.append("|");
                        sb.append(alUserInfo.getUserId());
                        sb.append("|");
                        sb.append(alUserInfo.getServiceType());
                        sb.append("|");
                        sb.append(alUserInfo.getPayMode());
                        sb.append("|");
                        sb.append(alUserInfo.getProductId());
                        sb.append("|");
                        sb.append(alUserInfo.getProductMode());
                        sb.append("|");
                        sb.append(alUserInfo.getInnetDate());
                        sb.append("|");
                        sb.append(alUserInfo.getInnetMonths());
                        sb.append("|");
                        sb.append(alUserInfo.getIsCard());
                        sb.append("|");
                        sb.append(alUserInfo.getIsInnet());
                        sb.append("|");
                        sb.append(alUserInfo.getIsThisAcct());
                        sb.append("|");
                        sb.append(alUserInfo.getIsThisBreak());
                        sb.append("|");
                        sb.append(alUserInfo.getCloseDate());
                        sb.append("|");
                        sb.append(alUserInfo.getCustId());
                        sb.append("|");
                        sb.append(alUserInfo.getUserIdEn());
                        sb.append("|");
                        sb.append(alUserInfo.getIsGrpMbr());
                        sb.append("|");
                        sb.append(alUserInfo.getUserStatus());
                        sb.append("|");
                        sb.append(alUserInfo.getIsStat());
                        sb.append("|");
                        sb.append(alUserInfo.getChannelId());
                        sb.append("|");
                        sb.append(alUserInfo.getStopType());
                        sb.append("|");
                        sb.append(alUserInfo.getProvId());

                        hbaseMap.put(k, sb.toString());
//                            HdfsUtil.writeLine2Hdfs(fileOutputStreamB, sb.toString());
//                        logger.info("-----sb------" + sb.toString());
                    }
                }
            } else {
                logger.info("未查询到结果!");
            }
        }
        return  hbaseMap;
    }
}
