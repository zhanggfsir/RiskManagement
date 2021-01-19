package com.unicom.testDemo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.List;

public class tmpTest {
    public static void main(String[] args) {
        String seperator = "|";
        ArrayList<String> datalist = new ArrayList<>();
        datalist.add("lizongsheng|50.0|男");
        datalist.add("zhoujielun|32|男");
        datalist.add("Victoria|18|女");
        //获得连接对象，创建Table


        List<Get> getList = new ArrayList<>();
        //TODO 传入的账期可能是6位的 此处 换成8位的 ！ 注意账期的长度
        for (String line : datalist) {

            String[] dataArray = StringUtils.splitPreserveAllTokens(line, seperator);

            //获得主键
            String nameKey = dataArray[0].toUpperCase();
            System.out.println(nameKey);
        }
    }
}
