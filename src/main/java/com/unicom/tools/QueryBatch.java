package com.unicom.tools;

import com.unicom.entity.ClassInstance;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.service.GetConfigInfo;
import com.unicom.utils.HdfsUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;


public class QueryBatch {
    private static Logger logger = LoggerFactory.getLogger(QueryBatch.class);
    private static final Object lock = new Object();
    // 从hfds中查得的数据在 saveHdfsPath/tableName 319查得的数据在saveHdfsPath/tableName319
    private static String saveHdfsPath="/user/ubd_test/ubd_risk_test.db/dxc/";     // /user/ubd_test/ubd_risk_test.db/dxc/zgf
    private static String implMethod="put2Hdfs";

    private static FSDataOutputStream hiveOutputStream ;
    private static FSDataOutputStream errorOutputStream ;
    private static FSDataOutputStream successOutputStream ;

    private static FileSystem fs;
    private static RemoteIterator<LocatedFileStatus> remoteIterator ;
    private static GetConfigInfo getConfigInfo;
    private static LoadColumnInfo loadColumnInfo;
    private static ClassInstance classInstance;

    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        if(args.length!=3){
            logger.error("args: zkName[319],tableName[user_daily_msisdn],columnName[ui] account[20190922]");
            System.exit(1);
        }
        String tableName=args[0];
        String columnName=args[1];
        String account=args[2];

        // 获得文件列表
        String xml319xl="319xml";
        int _10000Row=10000;
        List<Path> pathList=new ArrayList<>();
        HashSet<String> lineSet=new HashSet();

        init(tableName,columnName,account,xml319xl);

        while (remoteIterator.hasNext()) {
            LocatedFileStatus file =  remoteIterator.next();
            pathList.add(file.getPath());
        }
        int fileNum=pathList.size();
        int fileReaderCount=(int)Math.ceil(10000.0/fileNum);

        lineSet = getLineList(pathList, lineSet, fileReaderCount,_10000Row);

        classInstance.getImplClass().getDeclaredMethod(implMethod,FileSystem.class , LoadColumnInfo.class,HashSet.class,String.class ,String.class ,FSDataOutputStream.class, FSDataOutputStream.class,FSDataOutputStream.class)
                .invoke(classInstance.getInstance(),fs, loadColumnInfo, lineSet,tableName,account,hiveOutputStream,errorOutputStream,successOutputStream);
        close();
    }

    private static void init( String tableName, String columnName,String account,String xml319) throws IOException {

        fs=HdfsUtil.getFs(xml319);

        getConfigInfo=new GetConfigInfo();
        loadColumnInfo=getConfigInfo.getLoadColumnInfo(tableName,columnName);
        //路径拼接 得到HDFS上的文件
        String hdfsPath = getPath(loadColumnInfo.getFilePath(),loadColumnInfo.getPartitions(),account);
        remoteIterator=fs.listFiles(new Path(hdfsPath), true);

        classInstance=getClassInstance(loadColumnInfo);
        // 文件写出对象

        hiveOutputStream=HdfsUtil.getFileOutPutStream(fs,saveHdfsPath+tableName+"_"+columnName+account+"_hive");
        errorOutputStream= HdfsUtil.getFileOutPutStream(fs,saveHdfsPath+tableName+"_"+columnName+account+"_error");
        successOutputStream= HdfsUtil.getFileOutPutStream(fs,saveHdfsPath+tableName+"_"+columnName+account+"_success");

    }

    private static HashSet<String> getLineList(List<Path> pathList, HashSet<String> lineSet, int fileReaderCount,int _10000Row) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        BufferedReader bufferedReader=null;
        for (Path path:pathList){
//            logger.info(path.toString()+" ");
            String suffix=path.toString().substring(path.toString().lastIndexOf(".")+1);
            // 读文件
            InputStream inputStream = fs.open(path, 8192);

            if (suffix.equalsIgnoreCase("gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
                lineSet=parseLine2List(bufferedReader,fileReaderCount, path.toString() ,lineSet,_10000Row);
                gzipInputStream.close();
            } else if (suffix.equalsIgnoreCase("bzip2")) {
                BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(bZip2CompressorInputStream));
                lineSet=parseLine2List(bufferedReader,fileReaderCount, path.toString() ,lineSet,_10000Row);
                bZip2CompressorInputStream.close();
            }  else if (suffix.equalsIgnoreCase("snappy")) {
                int bufferSize = 262144;
                CompressionInputStream inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
                DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
                bufferedReader = new BufferedReader(new InputStreamReader(inflateIn));
                lineSet=parseLine2List(bufferedReader,fileReaderCount, path.toString() ,lineSet,_10000Row);
                inflateIn.close();
                inflateFilter.close();
            } else {  //读txt   认为传入的都是正确格式的文件
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                lineSet=parseLine2List(bufferedReader,fileReaderCount, path.toString() ,lineSet,_10000Row);
            }
            inputStream.close();
        }
        bufferedReader.close();
        return lineSet;
    }
    private static String getPath(String prefixFilePath,String partitions,String account) throws IOException {
        String hdfsPath=null;
        String partitionArray[]= StringUtils.splitPreserveAllTokens(partitions,"|");
        account=StringUtils.substring(account,0,8);
        logger.info("prefixFilePath,{},partitions,{}",prefixFilePath,partitions);

        logger.info("partitionArray.length "+partitionArray.length);
        //有 2个分区的情况
        //1. month_id string, prov_id string
        //todo 如果是改造后 分区为0的目录下 只有号段表的入库  需要把个性化的再捋一次。别的不用关注了。
        if(partitionArray.length==0){
            //应用场景 号段表的入库 无分区
            remoteIterator = fs.listFiles(new Path(prefixFilePath), true);
        } else if (partitionArray.length==1){
            // 应用场景  当前仅针对  基站码表 lf_xl_dim.dim_m_cell_combine_all 分区只有1个 prov_id
            //注 ：完全可以把此处的解析放在 partitions.length==0 里，在else的逻辑里直接执行。如果之后继续添加解析逻辑，此处可以做优化！
            if(partitionArray[0].equalsIgnoreCase("prov_id") || partitionArray[0].equalsIgnoreCase("mac")){
                return prefixFilePath;
            }else if (partitionArray[0].equalsIgnoreCase("prov_id_part") ){
                return  prefixFilePath;
            }else{
                logger.info("仅有1个分区 且分区是prov_id");
            }
        }else if (partitionArray.length==2){
            String partId=StringUtils.substring(account,0,6);
            if(partitionArray[0].equalsIgnoreCase("part_id") || partitionArray[0].equalsIgnoreCase("month_id")){
                if(partitionArray[1].equalsIgnoreCase("prov_id")){
                    hdfsPath=prefixFilePath+"/"+partitionArray[0]+"="+partId;
                    return hdfsPath;
                }else if(partitionArray[1].equalsIgnoreCase("day_id")){     // 仅精讯网约车 PJingxunInternetTaxi 2个分区 month_id后是day_id
                    String dayIdValue=account.substring(6,8);
                    hdfsPath=prefixFilePath+"/"+partitionArray[0]+"="+partId+"/"+partitionArray[1]+"="+dayIdValue;
                    return  hdfsPath;
                }
                else{
                    logger.info("仅有2个分区 且第二个分区不是prov_id的暂时不支持");
                }
            }
        }
        // 有3个分区的情况
        //1. month_id string, day_id string, prov_id string
        //2. month_id string, prov_id string, sa_type string comment 'cb or mb'
        else if(partitionArray.length==3){
            String partId=StringUtils.substring(account,0,6);
            if(partitionArray[0].equalsIgnoreCase("part_id") || partitionArray[0].equalsIgnoreCase("month_id")){
                if(partitionArray[1].equalsIgnoreCase("day_id")){
                    String dayId=account.substring(6,8);
                    String partitionsPartId=partitionArray[0];
                    String partitionsDayId=partitionArray[1];
                    hdfsPath=prefixFilePath+"/"+partitionsPartId+"="+partId+"/"+partitionsDayId+"="+dayId;
                    return hdfsPath;
                }else if(partitionArray[1].equalsIgnoreCase("prov_id")){
                    String partitionsPartId=partitionArray[0];
                    hdfsPath=prefixFilePath+"/"+partitionsPartId+"="+partId;
                    return hdfsPath;
                }else{
                    logger.info("仅有3个分区 且第二个分区不是 day_id/prov_id 的暂时不支持");
                }
            }
        } else{
            logger.error("分区异常，请检查当前分区 "+ Arrays.toString(partitionArray));
        }
        return hdfsPath;
    }
    private static HashSet parseLine2List(BufferedReader bufferedReader,int fileReaderCount, String path,HashSet<String> lineSet,int _10000Row) throws IOException {
        for (int i=0;i<fileReaderCount;i++){  // 10000/fileList.size()
            String line;
            if ((line = bufferedReader.readLine()) != null) {
                if(lineSet.size()>=_10000Row)
                    return  lineSet;
                lineSet.add(line+"|"+path);
            }
        }
        return lineSet;
    }

    private static ClassInstance getClassInstance(LoadColumnInfo loadColumnInfo) {

        File jarFile = new File(loadColumnInfo.getJarName().trim());
        // 从URLClassLoader类中获取类所在文件夹的方法，jar也可以认为是一个文件夹
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }
        // 获取方法的访问权限以便写回
        boolean accessible = method.isAccessible();
        try {
            method.setAccessible(true);
            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            URL url = jarFile.toURI().toURL();
            method.invoke(classLoader, url);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            method.setAccessible(accessible);
        }

        Class<?> implClass;
        Object instance;
        try {
            // "com.unicom.queryImpl.UiBatchQueryImpl" com.unicom.queryImpl.CiBatchQueryImpl
            System.out.println(">> bijiao");
            System.out.println("com.unicom.queryImpl.CiBatchQueryImpl".equals(loadColumnInfo.getQueryClassName().trim()));
            implClass = Class.forName(loadColumnInfo.getQueryClassName().trim());
            instance = implClass.newInstance();
            return new ClassInstance(implClass,instance);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return  null;
    }

    private static void close() throws IOException {
        hiveOutputStream.close();
        errorOutputStream.close();
        successOutputStream.close();
        fs.close();
    }
}
