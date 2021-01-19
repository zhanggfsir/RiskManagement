package com.unicom.testDemo._10000r;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class _5r {
    /*
    读取每个文件的前5行
     */
    private static List<File> fileList = new ArrayList<File>();
    public static void main(String[] args) throws IOException {
        _5r.traverseFolder("F:\\ChinaUnicom\\riskManage\\new\\RiskManagementDoc\\02_设计\\hbase库级别改造\\hbase_test_data");
        System.out.println(fileList.size());
        System.out.println(10000/22);

        for (File file :fileList){
            // 读文件  正式环境中读文件 文件类型可能有多种  包装一个函数就好了
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"));

            for (int i=0;i<5;i++){  // 正式环境中 每行取出 10000/fileList.size()
                String line;
                if ((line = bufferedReader.readLine()) != null) {
                        System.out.println(line);   // 读的文件2个去处： 1写到同名_tmp文件下 ；2截取得到Key去Hbase中查询，将查询到的结果写到_tmp_hbase中
                }
            }
        }
    }

    public  static void traverseFolder(String path) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
//                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                        fileList.add(file2);
//                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

}
