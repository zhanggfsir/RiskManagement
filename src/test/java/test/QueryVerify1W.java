package test;

import com.unicom.entity.ClassInstance;
import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import com.unicom.service.GetConfigInfo;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
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
import java.util.List;
import java.util.zip.GZIPInputStream;

//import static sun.awt.image.PixelConverter.Argb.instance;

//  /hbase/data/default/zhanggf/e352db9dfbef498da334adc190be472d/course/f6a79fff382d40bc90b649737156d844
public class QueryVerify1W {
    // 1w 行数据
    private static String saveFilePathHdfs = "/user/lf_by_pro/zba_dwa.db/zhanggf/x.txt";
    // query hbase
    private static String saveFilePathHdfsB = "/user/lf_by_pro/zba_dwa.db/zhanggf/y.txt";
    private static String saveFilePathHdfsC = "/user/lf_by_pro/zba_dwa.db/zhanggf/z.txt";

    private static Logger logger = LoggerFactory.getLogger(QueryVerify1W.class);
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
        RemoteIterator<LocatedFileStatus> remoteIterator = null;

        //获得table
        Table table=getTable(zkInfo,tableName);
        // 获得文件列表
        //TODO mypath 需要拼接形成路径 partitions 看每次能不能偷懒 直接输入地址拉到了    loadColumnInfo.getFilePath();
        String mypath="/user/lf_by_pro/zba_dwa.db/dwa_v_d_cus_al_user_info/part_id=201909/day_id=22/prov_id=010/";

        remoteIterator = fs.listFiles(new Path(mypath), true);
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
        // 文件写出对象
        FSDataOutputStream fileOutputStream = null;

        Path hdfsPath = new Path(saveFilePathHdfs);

        if (!fs.exists(hdfsPath)) {
            fileOutputStream = fs.create(hdfsPath,false);
        }else{
            fileOutputStream = fs.append(hdfsPath);
        }

        // 文件写出对象
        FSDataOutputStream fileOutputStreamB = null;

        Path hdfsPathB = new Path(saveFilePathHdfsB);

        if (!fs.exists(hdfsPathB)) {
            fileOutputStreamB = fs.create(hdfsPathB,false);
        }else{
            fileOutputStreamB = fs.append(hdfsPathB);
        }

        // 文件写出对象
        FSDataOutputStream fileOutputStreamC = null;

        Path hdfsPathC = new Path(saveFilePathHdfsC);

        if (!fs.exists(hdfsPathC)) {
            fileOutputStreamC = fs.create(hdfsPathC,false);
        }else{
            fileOutputStreamC = fs.append(hdfsPathC);
        }

        for (Path path:pathList){
            logger.info(path.toString()+" ");
            String suffix=path.toString().substring(path.toString().lastIndexOf(".")+1);
            // 读文件
            String tmppath=path.getName();
            InputStream inputStream = fs.open(path, 8192);
            BufferedReader bufferedReader=null;

            if (suffix.equalsIgnoreCase("gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream,"UTF-8"));
                //可以提出函数 getLineAndWrite2Hdfs
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath ,fileOutputStream,fileOutputStreamB,fileOutputStreamC);
                gzipInputStream.close();
            } else if (suffix.equalsIgnoreCase("bzip2")) {
                BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(bZip2CompressorInputStream,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath, fileOutputStream,fileOutputStreamB,fileOutputStreamC );
                bZip2CompressorInputStream.close();
            }  else if (suffix.equalsIgnoreCase("snappy")) {
                int bufferSize = 262144;
                CompressionInputStream inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
                DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
                bufferedReader = new BufferedReader(new InputStreamReader(inflateIn,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath,fileOutputStream, fileOutputStreamB,fileOutputStreamC );
                inflateIn.close();
                inflateFilter.close();
            } else {  // (suffix.equalsIgnoreCase("txt"))  认为传入的都是正确格式的文件
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath ,fileOutputStream,fileOutputStreamB,fileOutputStreamC);
            }
//            connection.close();
            fileOutputStream.close();
            fileOutputStreamB.close();
            fileOutputStreamC.close();

            bufferedReader.close();
            inputStream.close();
        }
    }

    private static void getLineAndWrite2Hdfs(FileSystem fs,BufferedReader bufferedReader,ClassInstance classInstance,
                                             LoadColumnInfo loadColumnInfo,String account,   int fileReaderCount, String path,FSDataOutputStream fileOutputStream,FSDataOutputStream fileOutputStreamB,FSDataOutputStream fileOutputStreamC) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        for (int i=0;i<fileReaderCount;i++){  // 10000/fileList.size()
            String line;
            line = bufferedReader.readLine();
//            if ((line = bufferedReader.readLine()) != null) {
                // 从Hive中查并写到HDFS
                write2Hdfs(fs,path,line,fileOutputStream);

                String implMethod="put2Hdfs";
                // Path.class
            // TODO 新加了 FileSystem fs  FSDataOutputStream fileOutputStreamB,FSDataOutputStream fileOutputStreamC | fs,fileOutputStreamB,fileOutputStreamC
            classInstance.getImplClass().getDeclaredMethod(implMethod, String.class,LoadColumnInfo.class, String.class,String.class ,FileSystem.class , FSDataOutputStream.class,FSDataOutputStream.class).invoke(classInstance.getInstance(), line, loadColumnInfo, path.toString(), account,fs,fileOutputStreamB,fileOutputStreamC);
        }
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
            implClass = Class.forName("com.unicom.queryImpl.UiBatchQueryImpl".trim());
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

    public static void write2Hdfs(FileSystem fs,String  filePathHdfs,String line,FSDataOutputStream fileOutputStream) {
        //可以实现 append 追加写
        //TODO  路径需要怎么拼接呢

        writeLine(fs,filePathHdfs,line,fileOutputStream);
    }

    private static Table getTable(ZkInfo zkInfo,String tableName) {
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

    private static void writeLine(FileSystem fs, String filePathHdfs,String line,FSDataOutputStream fsDataOutputStream) {

            //TODO 把line写入
        try {
//            fsDataOutputStream.write
            fsDataOutputStream.writeBytes(line+"\n");
//            fileOutputStream.writeUTF(new File(filePathHdfs.toString()).getName()+"-->"+line);
//            fileOutputStream.writeUTF(line); //二进制
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static FileSystem getFs() {

        Configuration hdfsConf = new Configuration();
        // HbaseTest_6  319xml
        InputStream hdfdSite= QueryVerify1W.class.getClassLoader().getClass().getResourceAsStream("/319xml/hdfs-site.xml");
        InputStream coreSite= QueryVerify1W.class.getClassLoader().getClass().getResourceAsStream("/319xml/core-site.xml");
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

}
