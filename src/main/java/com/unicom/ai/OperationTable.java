package com.unicom.ai;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


//import com.itheima.io.filter.MyFilenameFilter;


import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import javax.imageio.ImageIO;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;


public class OperationTable {
    static BASE64Encoder encoder = new sun.misc.BASE64Encoder();
    static BASE64Decoder decoder = new sun.misc.BASE64Decoder();
    private static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }


    public static void main(String[] args) throws IOException {
//     创建表
        createTable("test","time");




//     根据文件夹将内部所有照片导入
//     File file = new File("E:\\qwe");//文件位置
//     File files[] = file.listFiles(new MyFilenameFilter());
//      for(File f : files)
//      {
//       String name = f.getName();
//       String year = name.substring(0,4);
//       String month = name.substring(4,6);
//       String day = name.substring(6,8);
//       String hour = name.substring(8,10);
//       String minute = name.substring(10,12);
//       String address = f.toString();
//       String a=getImageBinary(address);
//       String[] cols = { hour+":"+minute};//列
//         String[] colsValue = {a};//值
//         addData("test",year+"-"+month+"-"+day, cols, colsValue);//表名，行健，列，值
//       System.out.println(f);
//      }







//      //获得数据
//     Scanner scan = new Scanner(System.in);
//      String read = scan.nextLine();
//      String read1 = scan.nextLine();
//       String a=getData("test", read,"time",read1);
        //将从hbase中获得二进制转化成照片放在本地

//       base64StringToImage(a,"E://qwe//123");//第四个参数输出地址




        //举例
//     String[] cols = {"21:03","12:02"};//列簇
//     String[] colsValue = {"2016-03-03","2"};//值
//     addData("test","2015-06-03", cols, colsValue);//表名，行健，列簇，值


    }
    //在Hasee里建立表
    private static void createTable(String TableName,String family) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {


        HBaseAdmin admin = new HBaseAdmin(conf);// 新建一个数据库管理员
        if (admin.tableExists(TableName)) {
            System.out.println("table is exist!");
            System.exit(0);
        } else {


            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TableName));
            desc.addFamily(new HColumnDescriptor(family));
            admin.createTable(desc);
            admin.close();
            System.out.println("create table Success!");
        }
    }


    //插入多条数据
    private static void addData(String tableName,String rowKey, String[] column1, String[] value1) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//

        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();


        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("time")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }


        }
        table.put(put);
        System.out.println("add data Success!");
    }

    //将一张照片转化成二进制返回
    static String getImageBinary(String address) {
        File f = new File(address);
        BufferedImage bi;
        try {
            bi = ImageIO.read(f);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bi, "jpg", baos); //经测试转换的图片是格式这里就什么格式，否则会失真
            byte[] bytes = baos.toByteArray();

            return encoder.encodeBuffer(bytes).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //将二进制转换成照片存进电脑
    static void base64StringToImage(String base64String,String address) {
        try {
            byte[] bytes1 = decoder.decodeBuffer(base64String);

            ByteArrayInputStream bais = new ByteArrayInputStream(bytes1);
            BufferedImage bi1 = ImageIO.read(bais);

            File w2 = new File(address+"//"+"查找文件"+".jpg");// 可以是jpg,png,gif格式
            ImageIO.write(bi1, "jpg", w2);// 不管输出什么格式图片，此处不需改动
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据表名和行健查等找图片的二进制
    private static String getData( String tableName,String rowKey,String family,String qualifier) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));


        // Instantiating Get class
        Get g = new Get(Bytes.toBytes(rowKey));


        // Reading the data
        Result result = table.get(g);

        String a = new String(result.getValue(family.getBytes(), qualifier.getBytes()));


        System.out.println("get data Success!");
        return a;
    }



}
//————————————————
//        版权声明：本文为CSDN博主「DT弄潮儿」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
//        原文链接：https://blog.csdn.net/BD_AI_IoT/article/details/78396898
//public class OperationTable {
//}
