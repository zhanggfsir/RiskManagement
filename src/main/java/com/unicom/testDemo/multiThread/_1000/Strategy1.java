package com.unicom.testDemo.multiThread._1000;

import java.util.LinkedList;
import java.util.List;

/**
 * 策略1: 每次开启n个线程，等待n个线程全部结束之后，再开启下n个线程，每个线程处理一个task.
 * 缺陷：需要等待其他n - 1个线程结束后，才能同时启动下n个线程
 */
public class Strategy1 {

    public static void main(String[] args) {
        List<Task> tasks = TaskProducer.produce(1000);
        handleTasks(tasks, 10);
        System.out.println("All finished");
    }

    public static void handleTasks(List<Task> tasks, int threadCount) {
        int taskCount = tasks.size();

        List<Thread> threadHolder = new LinkedList<Thread>();
        for(int i = 0; i < taskCount; i += threadCount) {
            for(int j = 0; j < threadCount && (i + j) < taskCount; j ++) {
                Thread thread = new Thread(new TaskHandler(tasks.get(i + j)));
                threadHolder.add(thread);
                thread.start();
            }

            waitToFinish(threadHolder);
            threadHolder.clear();
        }
    }

    public static void waitToFinish(List<Thread> threadHolder) {
        while(true) {
            boolean allFinished = true;
            for(Thread thread : threadHolder) {
                allFinished = allFinished && !thread.isAlive();
            }

            if(allFinished) {
                break;
            }
        }
    }

    public static class TaskHandler implements Runnable {
        private Task task;

        public TaskHandler(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.start();
        }
    }

}
