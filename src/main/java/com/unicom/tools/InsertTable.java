package com.unicom.tools;

import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.LogAccoutInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.service.GetConfigInfo;
import com.unicom.service.Hdfs2HBase;
import com.unicom.utils.CommonUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class InsertTable {
    private static Logger logger = LoggerFactory.getLogger(InsertTable.class);

    private static final Object lock = new Object();
    /**
     * tableName 为必填
     * @param args  zkName tableName column_name(f:mf mf 一个表中会有多个列) accout
     */
    public static void main(String[] args) throws IOException {
        String zkName=null;
        String tableName=null;
        String columnName=null;
        String account=null;    

        if(args.length==4){
            zkName=args[0];
            tableName=args[1];
            columnName=args[2];
            account=args[3];
        }else{
            logger.error("参数个数只能是4个：zkName tableName columnName account");
            System.exit(-1);
        }

        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);
        LoadColumnInfo loadColumnInfo=getConfigInfo.getLoadColumnInfo(tableName,columnName);
        // 插入日志时判断 如果账期是6位的 记为M；如果账期是8位的 记为D
        //LogAccoutInfo logAccoutInfo=new LogAccoutInfo(createTableInfo.getTableId(),loadColumnInfo.getColumnId(),account);

        //加載類對象
        //獲得jar ../jar/risk-1.0-jar-with-dependencies.jar";
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

        //"com.unicom.impl.individualizationImpl.PJingxunInternetTaxi"
        Class<?> implClass = null;
        Object instance = null;
        try {
            implClass = Class.forName(loadColumnInfo.getClassName().trim());
            instance = implClass.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }


        if (loadColumnInfo.isHdfs()){
            putHdfsFile(zkInfo,createTableInfo,loadColumnInfo,tableName,account,implClass,instance);
            //当前日志通过脚本插入
            //getConfigInfo.addLogAccountInfo(logAccoutInfo);
        }else
            logger.info("当前只支持从HDFS数据入库 ！");
            //目前风控点查询只支持从HDFS
           // putLocalFile(zkInfo,createTableInfo,loadColumnInfo,tableName,account);
    }

    private static void putLocalFile(ZkInfo zkInfo, CreateTableInfo createTableInfo, LoadColumnInfo loadColumnInfo, String tableName, String account) {
        File file = new File(loadColumnInfo.getFilePath());
        CommonUtil commonUtil=new CommonUtil();
        List<String> pathList = commonUtil.getFilePath(file);
        String[] hdfsAddress = loadColumnInfo.getNameService().split(",");

        String partitions[]=StringUtils.splitPreserveAllTokens(loadColumnInfo.getPartitions(),"|");

        ExecutorService executorService = Executors.newFixedThreadPool(loadColumnInfo.getThreadNum());
        for (String path : pathList) {
            for (String str : hdfsAddress) {
            Hdfs2HBase hdfs2HBase = new Hdfs2HBase(zkInfo,createTableInfo,loadColumnInfo,path,tableName, account);
            executorService.execute(() -> {
                try {
                    hdfs2HBase.runFromLocalPath();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            });
            }
        }

        executorService.shutdown();
        while (true) {
            try {
                if (executorService.isTerminated()) {
                    logger.info("所有的子线程都结束了");
                    break;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        logger.info("入库结束!");
    }

    private static void putHdfsFile(ZkInfo zkInfo,CreateTableInfo createTableInfo,LoadColumnInfo loadColumnInfo,String tableName,String account,Class<?> implClass,Object instance) throws IOException {
        Configuration hdfsConf = new Configuration();
        InputStream hdfdSite= InsertTable.class.getClassLoader().getClass().getResourceAsStream("/319xml/hdfs-site.xml");
        InputStream coreSite= InsertTable.class.getClassLoader().getClass().getResourceAsStream("/319xml/core-site.xml");
        hdfsConf.addResource(hdfdSite);
        hdfsConf.addResource(coreSite);
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = null;
        logger.info(loadColumnInfo.getNameService());
        String[] hdfsAddress = loadColumnInfo.getNameService().split(",");

        for (String hdfs : hdfsAddress) {
            try {
                hdfsConf.set("fs.defaultFS", hdfs);
                fs = FileSystem.get(hdfsConf);
                fs.getStatus();
                logger.info("hdfs连接成功!");
            } catch (IOException ex) {
                ex.printStackTrace();
                logger.error("hdfs连接失败,尝试连接下个namenode节点");
                System.exit(1);
            }
        }
        if (fs == null) {
            logger.error("获取文件列表失败");
            System.exit(1);
        }

        String partId=account.substring(0,6);
        RemoteIterator<LocatedFileStatus> remoteIterator = null;

        String partitions[]=StringUtils.splitPreserveAllTokens(loadColumnInfo.getPartitions(),"|");

        logger.info("partitions.length "+partitions.length);
        //有 2个分区的情况
        //1. month_id string, prov_id string
        if(partitions.length==0){
            String filePath=loadColumnInfo.getFilePath();
            remoteIterator = fs.listFiles(new Path(filePath), true);
        } else if (partitions.length==1){

            // 应用场景1 基站码表 dim_m_cell_combine_all 分区只有1个 prov_id_part
            // 应用场景2 dim_inc_msisdn_info 分区只有1个 mac
            // 应用场景3 特殊。 好懿春 只有一个 month_id,需要每月入库
            if(partitions[0].equalsIgnoreCase("month_id") ){
                String monthIdValue=account.substring(0,6);
                remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()+"/"+partitions[0]+"="+monthIdValue), true);
            }else {
                remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()), true);
            }
        }else if (partitions.length==2){
         if(partitions[0].equalsIgnoreCase("part_id") || partitions[0].equalsIgnoreCase("month_id")){
              if(partitions[1].equalsIgnoreCase("prov_id")){
                  remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()+"/"+partitions[0]+"="+partId), true);
              }else if(partitions[1].equalsIgnoreCase("day_id")){     // 仅精讯网约车 PJingxunInternetTaxi 2个分区 month_id后是day_id
                  String dayIdValue=account.substring(6,8);
                  remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()+"/"+partitions[0]+"="+partId+"/"+partitions[1]+"="+dayIdValue), true);
              }
              else{
                  logger.info("仅有2个分区 且第二个分区不是prov_id的暂时不支持");
              }
         }
    }
        // 有3个分区的情况
        //1. month_id string, day_id string, prov_id string
        //2. month_id string, prov_id string, sa_type string comment 'cb or mb'
        else if(partitions.length==3){
            if(partitions[0].equalsIgnoreCase("part_id") || partitions[0].equalsIgnoreCase("month_id")){
                if(partitions[1].equalsIgnoreCase("day_id")){
                    String dayId=account.substring(6,8);
                    String partitionsPartId=partitions[0];
                    String partitionsDayId=partitions[1];
                    remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()+"/"+partitionsPartId+"="+partId+"/"+partitionsDayId+"="+dayId), true);
                }else if(partitions[1].equalsIgnoreCase("prov_id")){
                    String partitionsPartId=partitions[0];
                    remoteIterator = fs.listFiles(new Path(loadColumnInfo.getFilePath()+"/"+partitionsPartId+"="+partId), true);
                }else{
                    logger.info("仅有3个分区 且第二个分区不是 day_id/prov_id 的暂时不支持");
                }
            }
        } else{
              logger.error("分区异常，请检查当前分区 "+ Arrays.toString(partitions));
        }


        ExecutorService executorService = Executors.newFixedThreadPool(loadColumnInfo.getThreadNum());
        AtomicLong atomicLong=new AtomicLong(0);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus file =  remoteIterator.next();
            Hdfs2HBase hdfs2HBase = new Hdfs2HBase(fs, zkInfo,createTableInfo,loadColumnInfo,file.getPath(),tableName, account,implClass, instance,atomicLong);
            executorService.execute(() -> {
                try {
                    hdfs2HBase.run();
                    //run(file.getPath(), fs,loadColumnInfo);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            });
        }

        executorService.shutdown();
        while (true) {
            try {
                if (executorService.isTerminated()) {
                    logger.info("所有的子线程都结束了");
                    break;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        logger.info("入库结束!");
        fs.close();
    }

}
