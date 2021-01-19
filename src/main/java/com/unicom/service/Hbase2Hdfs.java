package com.unicom.service;

import com.unicom.entity.ClassInstance;
import com.unicom.entity.CreateTableInfo;
import com.unicom.entity.LoadColumnInfo;
import com.unicom.entity.ZkInfo;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

import java.util.zip.GZIPInputStream;

public class Hbase2Hdfs  implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(Hbase2Hdfs.class);
//    private static final Object lock = new Object();

    private FileSystem fs;
    private ZkInfo zkInfo;
    private CreateTableInfo createTableInfo;
    private LoadColumnInfo loadColumnInfo;
    private ClassInstance classInstance;
    private Table table;
    private Path path;
    private int fileReaderCount;
    private String account;
    FSDataOutputStream fileOutputStream ;
    FSDataOutputStream fileOutputStreamB;
    FSDataOutputStream fileOutputStreamC;

    public Hbase2Hdfs() {
    }

    public Hbase2Hdfs(FileSystem fs, ZkInfo zkInfo, CreateTableInfo createTableInfo, LoadColumnInfo loadColumnInfo, ClassInstance classInstance, Table table, Path path, int fileReaderCount, String account, FSDataOutputStream fileOutputStream, FSDataOutputStream fileOutputStreamB, FSDataOutputStream fileOutputStreamC) {
        this.fs = fs;
        this.zkInfo = zkInfo;
        this.createTableInfo = createTableInfo;
        this.loadColumnInfo = loadColumnInfo;
        this.classInstance = classInstance;
        this.table = table;
        this.path = path;
        this.fileReaderCount = fileReaderCount;
        this.account = account;
        this.fileOutputStream = fileOutputStream;
        this.fileOutputStreamB = fileOutputStreamB;
        this.fileOutputStreamC = fileOutputStreamC;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {


        logger.info(path.toString()+" ");
        String suffix=path.toString().substring(path.toString().lastIndexOf(".")+1);
        // 读文件
        String tmppath=path.getName();
        InputStream inputStream = null;
        try {
            inputStream = fs.open(path, 8192);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader bufferedReader=null;

        if (suffix.equalsIgnoreCase("gz")) {
            GZIPInputStream gzipInputStream = null;
            try {
                gzipInputStream = new GZIPInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath ,fileOutputStream,fileOutputStreamB,fileOutputStreamC);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    gzipInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } else if (suffix.equalsIgnoreCase("bzip2")) {
            BZip2CompressorInputStream bZip2CompressorInputStream = null;
            try {
                bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(bZip2CompressorInputStream,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath, fileOutputStream,fileOutputStreamB,fileOutputStreamC );

            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    bZip2CompressorInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }  else if (suffix.equalsIgnoreCase("snappy")) {
            int bufferSize = 262144;
            CompressionInputStream inflateFilter = null;
            DataInputStream inflateIn=null;
            try {
                inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
                inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
                bufferedReader = new BufferedReader(new InputStreamReader(inflateIn,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath,fileOutputStream, fileOutputStreamB,fileOutputStreamC );
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    inflateFilter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    inflateIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } else {  // (suffix.equalsIgnoreCase("txt"))  认为传入的都是正确格式的文件
            try {
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
                getLineAndWrite2Hdfs(fs,bufferedReader,classInstance, loadColumnInfo,  account, fileReaderCount, tmppath ,fileOutputStream,fileOutputStreamB,fileOutputStreamC);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//            connection.close();

        try {
            fileOutputStream.close();
            fileOutputStreamB.close();
            fileOutputStreamC.close();
            bufferedReader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void getLineAndWrite2Hdfs(FileSystem fs, BufferedReader bufferedReader, ClassInstance classInstance,
                                             LoadColumnInfo loadColumnInfo, String account, int fileReaderCount, String path, FSDataOutputStream fileOutputStream, FSDataOutputStream fileOutputStreamB, FSDataOutputStream fileOutputStreamC) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

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

    public static void write2Hdfs(FileSystem fs,String  filePathHdfs,String line,FSDataOutputStream fileOutputStream) {
        //可以实现 append 追加写
        //TODO  路径需要怎么拼接呢

        writeLine(fs,filePathHdfs,line,fileOutputStream);
    }



    private static void writeLine(FileSystem fs, String filePathHdfs,String line,FSDataOutputStream fileOutputStream) {

        //TODO 把line写入
        try {
            fileOutputStream.writeBytes(line);
//            fileOutputStream.writeUTF(new File(filePathHdfs.toString()).getName()+"-->"+line);
//            fileOutputStream.writeUTF(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
