package com.unicom.testDemo._10000r;

public class Test {
    public static void main(String[] args) {
        /*
路径中一定会有. 不用担心数组越界
txt
gz
db/zhanggf/000000_0
         */
        //文件类型1
        String path="/user/lf_by_pro/zba_dwa.db/zhanggf/a.txt";
        System.out.println(path.substring(path.lastIndexOf(".")+1));
        String path2="/user/lf_by_pro/zba_dwa.db/zhanggf/a.gz";
        System.out.println(path2.substring(path2.lastIndexOf(".")+1));

        String path3="/user/lf_by_pro/zba_dwa.db/zhanggf/000000_0";
        System.out.println(path3.substring(path3.lastIndexOf(".")+1));


//        String path="/user/lf_by_pro/zba_dwa.db/zhanggf/a.txt";
//        System.out.println(path.substring(path.lastIndexOf(".")+1));
    }
}
