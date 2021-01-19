package test;

import com.unicom.entity.ClassInstance;
import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.service.GetConfigInfo;
import com.unicom.service.Hbase2Hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 开启多线程查询1W条  时间消耗18min
 */
//  /hbase/data/default/zhanggf/e352db9dfbef498da334adc190be472d/course/f6a79fff382d40bc90b649737156d844
public class QueryMulThread1W {
    // 1w 行数据
    public static String saveFilePathHdfs = "/user/lf_by_pro/zba_dwa.db/zhanggf/1.txt";
    // query hbase
    public static String saveFilePathHdfsB = "/user/lf_by_pro/zba_dwa.db/zhanggf/2.txt";
    public static String saveFilePathHdfsC = "/user/lf_by_pro/zba_dwa.db/zhanggf/3.txt";

    private static Logger logger = LoggerFactory.getLogger(QueryMulThread1W.class);
    private static final Object lock = new Object();
    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //咱也不知道也没有用，放在一个init方法中好了
        String zkName="319";
        String tableName="user_daily_msisdn"; //f:ui
        String columnName="ui";
        String account="20190922";

        if(args.length==4){
            zkName=args[0];
            tableName=args[1];
            columnName=args[2];
            account=args[3];
        }else{
            logger.error("参数个数只能是4个：zkName tableName columnName account");
//            System.exit(-1);
        }

        GetConfigInfo getConfigInfo=new GetConfigInfo();
        ZkInfo zkInfo=getConfigInfo.getZkInfo(zkName,tableName);
        CreateTableInfo createTableInfo=getConfigInfo.getConfigTableInfo(tableName);
        LoadColumnInfo loadColumnInfo=getConfigInfo.getLoadColumnInfo(tableName,columnName);

        //获得动态加载的实例
        ClassInstance classInstance=getClassInstance(loadColumnInfo);

        FileSystem fs=getFs();

        //获得table
        Table table=getTable(zkInfo,tableName);
        // 获得文件列表
        //TODO mypath 需要拼接形成路径 partitions 看每次能不能偷懒 直接输入地址拉到了    loadColumnInfo.getFilePath();
        String mypath="/user/lf_by_pro/zba_dwa.db/dwa_v_d_cus_al_user_info/part_id=201909/day_id=22/prov_id=010/";


        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(mypath), true);
        // 注意 Path File 的区别
        List<Path> pathList=new ArrayList<>();

        while (remoteIterator.hasNext()) {
            LocatedFileStatus file =  remoteIterator.next();
            pathList.add(file.getPath());
        }
        logger.info("------------filePath:"+pathList.toString()+"----------------");
        int fileNum=pathList.size();
        int fileReaderCount=(int)Math.ceil(10000.0/fileNum); //向上取整
        logger.info(fileNum+"-------- fileNum fileReaderCount --------"+" "+fileReaderCount);

        //*****************************************************************************
//        于此处 添加多线程逻辑 写文件的 逻辑不再放在子线程里，多次创建会报错
        //*****************************************************************************

        // 文件写出对象
        FSDataOutputStream fileOutputStream = null;

        Path hdfsPath = new Path(QueryMulThread1W.saveFilePathHdfs);

        try {
            if (!fs.exists(hdfsPath)) {
                fileOutputStream = fs.create(hdfsPath,false);
            }else{
                fileOutputStream = fs.append(hdfsPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 文件写出对象 B
        FSDataOutputStream fileOutputStreamB = null;

        Path hdfsPathB = new Path(QueryMulThread1W.saveFilePathHdfsB);

        try {
            if (!fs.exists(hdfsPathB)) {
                fileOutputStreamB = fs.create(hdfsPathB,false);
            }else{
                fileOutputStreamB = fs.append(hdfsPathB);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 文件写出对象 C
        FSDataOutputStream fileOutputStreamC = null;

        Path hdfsPathC = new Path(QueryMulThread1W.saveFilePathHdfsC);

        try {
            if (!fs.exists(hdfsPathC)) {
                fileOutputStreamC = fs.create(hdfsPathC,false);
            }else{
                fileOutputStreamC = fs.append(hdfsPathC);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



        ExecutorService excutorService= Executors.newFixedThreadPool(4);
        for (Path path:pathList){
            Hbase2Hdfs hbase2Hdfs=new Hbase2Hdfs(fs,zkInfo,createTableInfo,loadColumnInfo,classInstance,table,path,fileReaderCount,account,fileOutputStream,fileOutputStreamB,fileOutputStreamC);
            // ExecutorService exec = Executors.newFixedThreadPool(3);
            excutorService.execute(hbase2Hdfs);
        }
        excutorService.shutdown();
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

        Class<?> implClass = null;
        Object instance = null;
        try {
            // TODO  com.unicom.queryImpl.UiQueryImpl  com.unicom.queryImpl.UiQueryImpl
            // implClass = Class.forName(loadColumnInfo.getClassName().trim());
            implClass = Class.forName("com.unicom.queryImpl.UiQueryImpl".trim());
            instance = implClass.newInstance();
            return new ClassInstance(implClass,instance);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  null;
    }
    private static FileSystem getFs() {

        Configuration hdfsConf = new Configuration();
        // HbaseTest_6  319xml
        InputStream hdfdSite= QueryMulThread1W.class.getClassLoader().getClass().getResourceAsStream("/319xml/hdfs-site.xml");
        InputStream coreSite= QueryMulThread1W.class.getClassLoader().getClass().getResourceAsStream("/319xml/core-site.xml");
//      InsertTable.class.getClassLoader().getResource()
        hdfsConf.addResource(hdfdSite);
        hdfsConf.addResource(coreSite);
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.setBoolean("dfs.support.append", true);
        hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hdfsConf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        FileSystem fs = null;
        String nameService="hdfs://beh";
//        for (String hdfs : hdfsAddress) {
        try {
            hdfsConf.set("fs.defaultFS", nameService);
            fs = FileSystem.get(hdfsConf);
            fs.getStatus();
            logger.info("hdfs连接成功!");
        } catch (IOException ex) {
            ex.printStackTrace();
            logger.error("hdfs连接失败,尝试连接下个namenode节点");
            System.exit(1);
        }
//        }
        if (fs == null) {
            logger.error("获取文件列表失败");
            System.exit(1);
        }
        return fs ;
    }
    private static Table getTable(ZkInfo zkInfo, String tableName) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkInfo.getZkQuorum());
        configuration.set("zookeeper.znode.parent", zkInfo.getZkParent());
        Connection connection = null;
        Table table = null;
        synchronized (lock) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                table = connection.getTable(TableName.valueOf(tableName));
                return  table;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            lock.notifyAll();
        }
        if (table == null) {
            System.exit(1);
        }
        return null;
    }

}
