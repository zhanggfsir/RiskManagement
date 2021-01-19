package com.unicom.testDemo.multiThread._1000;

public class Task {

    private int id;

    public Task(int id) {
        this.id = id;
    }

    public void start() {
        System.out.println(Thread.currentThread().getName() + ": start to handle task " + id);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}