package com.unicom.testDemo.multiThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class WorkTask implements Runnable {
    public void run() {
        try {
//            int r = (int) (Math.random() * 10);
//            Thread.sleep(r * 1000);
            Thread.sleep( 5000);
            System.out.println(Thread.currentThread().getId() + " is over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

public class FixedThreadPoolTest {
        public static void main(String[] args) {
            ExecutorService exec = Executors.newFixedThreadPool(3);
            for (int i = 0; i < 20; i++) {
                exec.execute(new WorkTask());
            }
            exec.shutdown();
        }
}

/*
13 is over
11 is over
12 is over
13 is over
13 is over
12 is over
12 is over
11 is over
12 is over
13 is over
13 is over
12 is over
11 is over
13 is over
11 is over
12 is over
12 is over
13 is over
11 is over
12 is over
 */
