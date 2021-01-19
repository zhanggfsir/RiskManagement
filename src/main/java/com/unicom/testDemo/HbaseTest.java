package com.unicom.testDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author xiaojie.zhu
 */
public class HbaseTest {
    private Connection connection;
    private HTable table;
    HBaseAdmin admin;
    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper的地址，可以有多个，以逗号分隔
        configuration.set("hbase.zookeeper.quorum","dockerServer");
        //设置zookeeper的端口
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //创建hbase的连接，这是一个分布式连接
        connection = ConnectionFactory.createConnection(configuration);
        //获取hbase中的表
        table = (HTable) connection.getTable(TableName.valueOf("user"));

        //这个admin是管理table时使用的，比如说创建表
        admin = (HBaseAdmin) connection.getAdmin();
    }

    /**
     * 创建表，创建表只需要指定列族，不需要指定列
     * 其实用命令真的会更快，create 'user','info1','info2'
     */
    @Test
    public void createTable() throws IOException {
        //声明一个表名
        TableName tableName = TableName.valueOf("user");
        //构造一个表的描述
        HTableDescriptor desc = new HTableDescriptor(tableName);
        //创建列族
        HColumnDescriptor family1 = new HColumnDescriptor("info1");
        HColumnDescriptor family2 = new HColumnDescriptor("info2");
        //添加列族
        desc.addFamily(family1);
        desc.addFamily(family2);
        //创建表
        admin.createTable(desc);
    }


    /**
     * 添加数据
     * 对同一个row key进行重新put同一个cell就是修改数据
     */
    @Test
    public void insertUpdate() throws IOException {
        //构造参数是row key，必传
        for(int i = 0 ; i < 100 ; i ++){
            Put put = new Put(Bytes.toBytes("zhangsan_123" + i));
            //put.add()已经被弃用了
            //这里的参数依次为，列族名，列名，值
            put.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("name"),Bytes.toBytes("lisi" + i));
            put.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("age"),Bytes.toBytes(22 + i));
            put.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("sex"),Bytes.toBytes("男"));
            put.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("address"),Bytes.toBytes("天堂" + i));
            table.put(put);
            //table.put(List<Put>); //通过一个List集合，可以添加一个集合
        }

    }

    /**
     * 删除数据
     */
    @Test
    public void delete() throws IOException {
        Delete deleteRow = new Delete(Bytes.toBytes("zhangsan_1235")); //删除一个行

        Delete delete = new Delete(Bytes.toBytes("zhangsan_1235"));
        delete.addFamily(Bytes.toBytes("info1"));//删除该行的指定列族
        delete.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));//删除指定的一个单元


        table.delete(deleteRow);

        //table.delete(List<Delete>); //通过添加一个list集合，可以删除多个
    }


    /**
     * 查询单条数据
     * @throws IOException
     */
    @Test
    public void queryByKey() throws IOException {
        String rowKey = "zhangsan_1235";
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] address = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("address")); //读取单条记录
        byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name")); //读取单条记录
        byte[] sex = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("sex")); //读取单条记录
        byte[] age = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("age")); //读取单条记录
//        result.getValue()
        System.out.print(Bytes.toString(name) + ",");
        System.out.print(Bytes.toString(sex) + ",");
        System.out.print(Bytes.toString(address) + ",");
        System.out.print(Bytes.toInt(age) + ",");
        System.out.println();
    }

    /**
     * 全表扫描
     */
    @Test
    public void scanData() throws IOException {
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }


    /**
     * 区间扫描
     */
    @Test
    public void areaScanData() throws IOException {
        Scan scan = new Scan();

//        scan.withStartRow(Bytes.toBytes("zhangsan_1232")); //设置开始行
//        scan.withStopRow(Bytes.toBytes("zhangsan_12352")); //设置结束行
        //scan.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));//查询指定列
        scan.addFamily(Bytes.toBytes("info1"));//查询指定列族

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }


    /**
     * 全表扫描时加过滤器 --> 列值过滤器
     */
    @Test
    public void scanDataByFilter1() throws IOException {
        Scan scan = new Scan();
        /*
         * 第一个参数： 列族
         * 第二个参数： 列名
         * 第三个参数： 是一个枚举类型
         *              CompareOp.EQUAL  等于
         *              CompareOp.LESS  小于
         *              CompareOp.LESS_OR_EQUAL  小于或等于
         *              CompareOp.NOT_EQUAL  不等于
         *              CompareOp.GREATER_OR_EQUAL  大于或等于
         *              CompareOp.GREATER  大于
         */
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("name"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("zhangsan8"));
        scan.setFilter(singleColumnValueFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);

    }


    /**
     * 全表扫描时加过滤器 --> 前缀过滤器
     *
     * 这里的查询和单元内容没有关系，仅仅是匹配列名，比如有这样两个列 name1 和name2  ，通过这个过滤器，就会查询这两个列的所有数据，
     * 当然，其实这个方式和scan.addColumn差不多，
     * 且并会匹配到其它列族的列名
     *
     */
    @Test
    public void scanDataByFilter2() throws IOException {
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        Scan scan = new Scan();
        scan.setFilter(columnPrefixFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }

    /**
     * 全表扫描时添加过滤器  --> 多个列值前缀过滤器
     *
     *  这个过滤器和 ColumnPrefixFilter 过滤器差不多，但是能匹配多个前缀
     */

    @Test
    public void scanDataFilter3() throws IOException {
        Scan scan = new Scan();
        byte[][] prefix = new byte[][]{Bytes.toBytes("name"),Bytes.toBytes("add")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefix);

        scan.setFilter(multipleColumnPrefixFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }


    /**
     * row key查找
     */
    @Test
    public void scanByRowKey() throws IOException {
        //查找以指定内容开头的
        Filter rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^zhangsan_1239"));
        Scan scan = new Scan();
        scan.setFilter(rowKeyFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }


    /**
     * 使用多个过滤器，并合查找，可以同时设置多个过滤器
     *
     * 这里查找row key中，name列等于指定值的
     */
    @Test
    public void scanByFilterList() throws IOException {
        /*
         * 我们需要注意Operator这个参数，这是一个枚举类型，里面有两个类型
         *   Operator.MUST_PASS_ALL   需要通过全部的条件，也就是并且，and &&
         *   Operator.MUST_PASS_ONE   任何一个条件满足都可以，也就是或者，or ||
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //row key正则表达式的过滤器
        Filter rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^zhangsan_1239"));
        //列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("zhangsan9"));


        //把两个filter添加进filterList中
        filterList.addFilter(rowKeyFilter);
        filterList.addFilter(singleColumnValueFilter);


        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner resultScanner = table.getScanner(scan);
        printResult(resultScanner);
    }


    private void printResult(ResultScanner resultScanner) {
        for (Result result : resultScanner) {
            byte[] address = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("address")); //读取单条记录
            byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name")); //读取单条记录
            byte[] sex = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("sex")); //读取单条记录
            byte[] age = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("age")); //读取单条记录
            byte[] rowKey = result.getRow(); //获取rowKey
            System.out.print(Bytes.toString(rowKey) + ",");
            System.out.print(Bytes.toString(name) + ",");
            System.out.print(Bytes.toString(sex) + ",");
            System.out.print(Bytes.toString(address) + ",");
            System.out.print((age == null ? null : Bytes.toInt(age)) + ",");
            System.out.println();
        }
    }

    public static class _61_序列化二叉树 {
    }
}
