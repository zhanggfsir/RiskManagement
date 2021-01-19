package com.unicom.testDemo._10000r;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/*
写了一个遍历所有文件夹下文件的demo
 */
public class TraverseFolder {
    private static List<File> fileList = new ArrayList<File>();
    public static void main(String[] args) {
        TraverseFolder.traverseFolder("F:\\ChinaUnicom\\riskManage\\new\\RiskManagementDoc\\02_设计\\hbase库级别改造\\hbase_test_data");
        System.out.println("---------------------------");
        System.out.println(fileList);
    }
    private  static void traverseFolder(String path) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                        fileList.add(file2);
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

}
