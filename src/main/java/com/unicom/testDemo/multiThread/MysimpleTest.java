package com.unicom.testDemo.multiThread;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MysimpleTest {
    public static void main(String[] args) {
        String  path="hdfs://beh/user/lf_xl_bp/lf_xl_dim.db/dim_m_cell_combine_all/prov_id=011/000008_0";
        String provId=path.toString().split("=")[1].split("/")[0];
        System.out.println(provId);

        String partitions[]= StringUtils.splitPreserveAllTokens("zgf","|");
        System.out.println(partitions.length);



//创建一个可重用固定个数的线程池
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 10; i++) {
            fixedThreadPool.execute(new Runnable() {
                public void run() {
                    try {
                        //打印正在执行的缓存线程信息
                        System.out.println(Thread.currentThread().getName()+"正在被执行 ");
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
//        2147483647
        System.out.println(Integer.MAX_VALUE);

    }
}
