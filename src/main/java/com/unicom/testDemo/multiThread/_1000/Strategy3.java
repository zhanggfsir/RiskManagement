package com.unicom.testDemo.multiThread._1000;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 策略3: 使用ConcurrentLinkedQueue<Task>存储所有的Task，然后同时开启n个线程读取Queue.
 * 优点：充分利用所有线程，无等待
 * 缺点：需要将所有的task转移到Queue中，消耗一倍内存
 */
public class Strategy3 {

    public static void main(String[] args) {
        List<Task> tasks = TaskProducer.produce(1000);
        handleTasks(tasks, 10);
        System.out.println("All finished");
    }

    public static void handleTasks(List<Task> tasks, int threadCount) {
        Queue<Task> taskQueue = new ConcurrentLinkedQueue<Task>();
        taskQueue.addAll(tasks);

        List<Thread> threadHolder = new LinkedList<Thread>();
        for(int i = 0; i < threadCount; i ++) {
            Thread thread = new Thread(new TaskHandler(taskQueue));
            threadHolder.add(thread);
            thread.start();
        }

        waitToFinish(threadHolder);
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

        private final Queue<Task> tasks;

        public TaskHandler(Queue<Task> tasks) {
            this.tasks = tasks;
        }

        public void run() {
            while(!tasks.isEmpty()) {
                Task task = tasks.poll();
                if(task != null) {
                    task.start();
                }
            }
        }

    }

}