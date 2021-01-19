package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class countHbase {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, TableName.valueOf("T_REVIEW_MODULE"));
        LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
        AggregationClient aggregationClient = new AggregationClient(conf);

        Scan scan = new Scan( Bytes.toBytes("2018-07-01 12:12:12"), Bytes.toBytes("2018-07-27 12:12:12"));

//        Long count = aggregationClient.rowCount(hTable, columnInterpreter, scan);
    }



}
