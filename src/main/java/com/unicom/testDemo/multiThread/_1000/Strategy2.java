package com.unicom.testDemo.multiThread._1000;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 策略2: 将所有的task分割成n个子task列表，然后开启n个线程，每个线程处理一个子列表
 * 优点：无等待
 * 缺陷： 分割不均，无法充分利用所有的线程
 */
public class Strategy2 {

    public static void main(String[] args) {
        List<Task> tasks = TaskProducer.produce(1000);
        handleTasks(tasks, 10);
        System.out.println("All finished");
    }

    public static void handleTasks(List<Task> tasks, int threadCount) {
        List<List<Task>> splitTasks = splitTasksToNThreads(tasks, threadCount);

        List<Thread> threadHolder = new LinkedList<Thread>();
        for (List<Task> segment : splitTasks) {
            Thread thread = new Thread(new TaskHandler(segment));
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

    public static List<List<Task>> splitTasksToNThreads(List<Task> tasks, int threadCount) {
        List<List<Task>> splitTasks = new ArrayList<List<Task>>(threadCount);

        int taskCount = tasks.size();
        int taskPerThread = new BigDecimal(taskCount).divide(new BigDecimal(threadCount), RoundingMode.CEILING).intValue();

        for (int i = 0; i < taskCount; i += taskPerThread) {
            List<Task> segment = new LinkedList<Task>();
            for (int j = 0; j < taskPerThread && (i + j) < taskCount; j++) {
                segment.add(tasks.get(i + j));
            }

            splitTasks.add(segment);
        }

        tasks.clear();

        return splitTasks;
    }

    public static class TaskHandler implements Runnable {
        private List<Task> tasks;

        public TaskHandler(List<Task> tasks) {
            this.tasks = tasks;
        }

        @Override
        public void run() {
            for (Task task : tasks) {
                task.start();
            }
        }
    }

}