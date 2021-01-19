package com.unicom.testDemo._10000r;

import org.apache.hadoop.hbase.client.Put;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/*
读取文件 获取最后修改时间 按照文件修改时间顺序打印文件名
结果示例
2019-09-04 11:26:18:risk-1.0-jar-with-dependencies.jar-------->F:\ChinaUnicom\riskManage\new\RiskManagementDoc\02_设计\hbase库级别改造\hbase_test_data\jar\risk-1.0-jar-with-dependencies.jar
2019-09-04 11:25:36:risk-1.0.jar-------->F:\ChinaUnicom\riskManage\new\RiskManagementDoc\02_设计\hbase库级别改造\hbase_test_data\jar\risk-1.0.jar
2019-08-09 16:53:49:cz_dm_m_use_cz_info.txt-------->F:\ChinaUnicom\riskManage\new\RiskManagementDoc\02_设计\hbase库级别改造\hbase_test_data\cz_dm_m_use_cz_info.txt
 */
public class OrderByTimeGetFile {
    private static List<File> fileList = new ArrayList<File>();
    public static void main(String[] args) throws IOException {
        /**
         * 递归读取文件夹下所有文档
         * @author Administrator
         *
         */
//        public class FileLoop{

//            public static void main(String[] args) {
                List<File> list = fileReadLoop("F:\\ChinaUnicom\\riskManage\\new\\RiskManagementDoc\\02_设计\\hbase库级别改造\\hbase_test_data");

                for(int i=0; i<list.size(); i++){
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(list.get(i).lastModified()))+":"+list.get(i).getName() + "-------->"+list.get(i));

                    // 读文件
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(list.get(i)),"UTF-8"));

                    Put put=null;
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
//                        System.out.println(line);
                    }
                }
            }

            /**
             * 循环获取指定文件夹下的所有文件
             * @param path
             */
            private void loopReadDir(String path){
                File filePath = new File(path);
                File[] list = filePath.listFiles();
                if(list!=null && list.length>0){
                    for(int i=0; i<list.length; i++){
                        File f = list[i];
                        if(f.isFile() && !f.isHidden()){
                            fileList.add(f);
                        }else if(f.isDirectory() && !f.isHidden()){
                            loopReadDir(f.getPath());
                        }
                    }
                }
            }

            /**
             * 将文件按日期排序
             * @param
             * @return
             */
            private void sortFileList(){
                //按文件日期排序
                Collections.sort(fileList, new Comparator<File>() {
                    @Override
                    public int compare(File o1, File o2) {
                        if(o1.lastModified() > o2.lastModified()){
                            return -1;
                        }else if(o1.lastModified() == o2.lastModified()){
                            return 0;
                        }else{
                            return 1;
                        }
                    }
                });
            }

            /**
             * 调用静态方法
             * @param path
             * @return
             */
            public static List<File> fileReadLoop(String path) {
                OrderByTimeGetFile fileCon = new OrderByTimeGetFile();
                fileCon.loopReadDir(path);
                fileCon.sortFileList();
                return fileList;
            }
//        }

//    }
}
