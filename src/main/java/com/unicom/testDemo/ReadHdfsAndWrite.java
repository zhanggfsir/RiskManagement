package com.unicom.testDemo;


import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/*
读HDFS上的文件，并写入文件
java -Xmx4096m -cp risk-1.0-jar-with-dependencies.jar com.unicom.testDemo.ReadHdfsAndWrite
 */
public class ReadHdfsAndWrite {
    private static Logger logger = LoggerFactory.getLogger(ReadHdfsAndWrite.class);

    public static void main(String[] args) throws IOException {
        String readPath = "/user/ubd_test/ubd_risk_test.db/dxc/";
        String readPathTmp = "/user/ubd_test/ubd_risk_test.db/dxc/tmp";
        String writePath = "/user/ubd_test/ubd_risk_test.db/dxc/tmp3191";
        FileSystem fs= getFs("319xml");
        StringBuilder sb=new StringBuilder();
        List<String> pathList=new ArrayList<>();

        RemoteIterator<LocatedFileStatus> remoteIterator=fs.listFiles(new Path(readPath), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus file =  remoteIterator.next();
            pathList.add(file.getPath().toString());
        }

        for(String hdfsPath:pathList){
            InputStream inputStream = fs.open(new Path(hdfsPath), 8192);
            sb = getFromHdfs(inputStream,hdfsPath,sb);
            inputStream.close();
        }

        //将数据写入HDFS
        FSDataOutputStream save2HdfsfileOutputStream= getFileOutPutStream(fs,writePath);
        writeLine2Hdfs(save2HdfsfileOutputStream,sb.toString());

        //资源清理
        save2HdfsfileOutputStream.close();
        fs.close();

    }

    private static FileSystem getFs(String xml319) {

        Configuration hdfsConf = new Configuration();
        InputStream hdfdSite= ReadHdfsAndWrite.class.getClassLoader().getClass().getResourceAsStream("/"+xml319+"/hdfs-site.xml");
        InputStream coreSite= ReadHdfsAndWrite.class.getClassLoader().getClass().getResourceAsStream("/"+xml319+"/core-site.xml");
        hdfsConf.addResource(hdfdSite);
        hdfsConf.addResource(coreSite);
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.setBoolean("dfs.support.append", true);
        hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hdfsConf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        FileSystem fs = null;
        //String nameService="hdfs://beh";
        //for (String hdfs : hdfsAddress) {
        try {
            //hdfsConf.set("fs.defaultFS", loadColumnInfo.getNameService());
            fs = FileSystem.get(hdfsConf);
            fs.getStatus();
            logger.info("hdfs连接成功!");
        } catch (IOException ex) {
            ex.printStackTrace();
            logger.error("hdfs连接失败,尝试连接下个namenode节点");
            System.exit(1);
        }
        if (fs == null) {
            logger.error("获取文件列表失败");
            System.exit(1);
        }
        return fs ;
    }

    private static StringBuilder getFromHdfs( InputStream inputStream, String hdfsPath,StringBuilder sb) throws IOException {
        BufferedReader bufferedReader;
        String suffix = hdfsPath.substring(hdfsPath.lastIndexOf(".") + 1);
        if (suffix.equalsIgnoreCase("gz")) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
            sb = parseLine2List(bufferedReader, sb);
            gzipInputStream.close();
            bufferedReader.close();
        } else if (suffix.equalsIgnoreCase("bzip2")) {
            BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(inputStream);
            bufferedReader = new BufferedReader(new InputStreamReader(bZip2CompressorInputStream));
            sb = parseLine2List(bufferedReader, sb);
            bZip2CompressorInputStream.close();
            bufferedReader.close();
        } else if (suffix.equalsIgnoreCase("snappy")) {
            int bufferSize = 262144000;
            CompressionInputStream inflateFilter = new BlockDecompressorStream(inputStream, new SnappyDecompressor(bufferSize), bufferSize);
            DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
            bufferedReader = new BufferedReader(new InputStreamReader(inflateIn));
            sb = parseLine2List(bufferedReader, sb);
            inflateIn.close();
            inflateFilter.close();
            bufferedReader.close();
        } else {  //读txt   认为传入的都是正确格式的文件
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            sb = parseLine2List(bufferedReader, sb);
            bufferedReader.close();
        }
        return sb;
    }


    private static StringBuilder parseLine2List(BufferedReader bufferedReader, StringBuilder sb) throws IOException {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }
        return sb;
    }
    public static FSDataOutputStream getFileOutPutStream(FileSystem fs, String path) throws IOException {
        FSDataOutputStream fsfileOutputStream;
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            fsfileOutputStream = fs.create(hdfsPath,false);
        }else{
            fsfileOutputStream = fs.append(hdfsPath);
        }
        return fsfileOutputStream;
    }
    public static void writeLine2Hdfs(FSDataOutputStream fsDataOutputStream, String line) {
        try {
            fsDataOutputStream.writeBytes(line+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

