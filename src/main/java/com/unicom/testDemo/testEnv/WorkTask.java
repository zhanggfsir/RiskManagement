package com.unicom.testDemo.testEnv;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.exit;

class  WorkTask implements Runnable{
    static Logger logger = LoggerFactory.getLogger(WorkTask.class);
    private Table table;

    private  long start;
    private  long perSize;
    private AtomicLong ai;
    private CountDownLatch countDownLatch;
    private String qualifier;


    public WorkTask() { }
    public void setTableName(Table tableName) {
        this.table = tableName;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public void setPerSize(long perSize) {
        this.perSize = perSize;
    }

    public void setAi(AtomicLong ai) {
        this.ai = ai;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    @Override
    public void run() {
        List<Put> list = new ArrayList<>();
        long count=0;
        long sum=0;

        Put put=null;
        int flag=0;

        for(long number=start;number<(start+perSize);number++){
            switch (qualifier){
//                case "md5":
////                    put= Sha256Md5.getPut(number);
////                    break;
                case "pt":
                    put= Md5Plaintext.getPut(number);;
                    break;
                    default:
                        logger.info("列 {} 不正确，程序退出",qualifier);
                        exit(1);
                        break;
            }

            flag++;
            count++;
            if (put != null) {
                list.add(put);
                sum++;
                ai.getAndIncrement();
            }
            // 批量写入
            if (list.size() > Md5Plaintext.BatchCount) {
                try {
                    table.put(list);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("入库错误,丢弃数据 " );
                    exit(1);
                }
                list.clear();
            }

            if(flag%20000000 ==0){ //20000000
                logger.info("库:"+ "  /hbase_cx  "+", 记录数: " +flag + ", 入库记录数: " + sum);
                flag=0;
            }
        }
        // 清理资源
        try {
            table.put(list);
        } catch (Exception e) {
            e.printStackTrace();
            exit(1);
        }

        list.clear();
        logger.info("---------------------------入库完成---------------------------");
        logger.info("库:"+ "  /hbase_cx  "+", 记录数: " + count + ", 入库记录数: " + sum+" 总记录数为"+ai);

        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (countDownLatch != null) {
                countDownLatch.countDown();
            }
        }
    }
}
