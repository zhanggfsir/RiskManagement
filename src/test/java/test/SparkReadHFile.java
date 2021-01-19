//package com.unicom.tools;
//
//import java.io.IOException;
//import java.util.*;
//import java.util.Map.Entry;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.util.Base64;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import scala.Tuple2;
//
//public class SparkReadHFile {
//    private static String convertScanToString(Scan scan) throws IOException {
//        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//        return Base64.encodeBytes(proto.toByteArray());
//    }
//
//    public static void main(String[] args) throws IOException {
//        final String date=args[0];
//        //  final String date="123";
//        int max_versions = 1;
//        SparkConf sparkConf = new SparkConf().setAppName("sparkReadHfile");//.setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Configuration hconf = HBaseConfiguration.create();
//        hconf.set("hbase.rootdir", "/hbase");
//        hconf.set("hbase.zookeeper.quorum", "*:2181,*:2181,*:2181");
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("C"));
//        scan.setMaxVersions(max_versions);
//        hconf.set(TableInputFormat.SCAN, convertScanToString(scan));
//        Job job = Job.getInstance(hconf);
//        Path path = new Path("/snapshot");
//        String snapName ="***Snapshot";//快照名
//        TableSnapshotInputFormat.setInput(job, snapName, path);
//        JavaPairRDD<ImmutableBytesWritable, Result> newAPIHadoopRDD = sc.newAPIHadoopRDD(job.getConfiguration(), TableSnapshotInputFormat.class, ImmutableBytesWritable.class,Result.class);
//        List<String> collect = newAPIHadoopRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>(){
//            private static final long serialVersionUID = 1L;
//            public String call(Tuple2<ImmutableBytesWritable, Result> v1)
//                    throws Exception {
//                // TODO Auto-generated method stub
//                String newMac =null;
//                Result result = v1._2();
//                System.out.println("执行。。。");
//                if (result.isEmpty()) {
//                    return null;
//                }
//                String rowKey = Bytes.toString(result.getRow());
//                System.out.println("行健为："+rowKey);
//                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("C"));
//                Set<Entry<byte[], byte[]>> entrySet = familyMap.entrySet();
//                Iterator<Entry<byte[], byte[]>> it = entrySet.iterator();
//                String colunNmae =null;
//                String minDate="34561213";
//                while(it.hasNext()){
//                    colunNmae = new String(it.next().getKey());//列
//                    if(colunNmae.compareTo(minDate)<0){
//                        minDate=colunNmae;
//                    }
//                }
//
//                if (date.equals(minDate)) {
////                    row=rowKey.substring(4);
//                    newMac=rowKey;
//                    //ls.add(rowKey.substring(4));
//                    //bf.append(rowKey+"----");
//                }
//                return  newMac;
//            }
//        }).collect();
//        ArrayList<String> arrayList = new ArrayList<String>();
//        for (int i = 0; i < collect.size(); i++) {
//            if (collect.get(i) !=null) {
//                arrayList.add(collect.get(i));
//            }
//        }
//        System.out.println("新增mac数"+(arrayList.size()));
//
//    }
//}