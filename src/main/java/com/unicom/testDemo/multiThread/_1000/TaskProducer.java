package com.unicom.testDemo.multiThread._1000;

import java.util.LinkedList;
import java.util.List;

public class TaskProducer {

    public static List<Task> produce(int count) {
        List<Task> tasks = new LinkedList<Task>();

        for(int i = 0; i < count; i ++) {
            tasks.add(new Task(i + 1));
        }

        return tasks;
    }

}