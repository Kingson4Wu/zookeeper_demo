package com.kxw.observer;

import org.springframework.stereotype.Component;

@Component
public class CommonExecutorService {

    /*ExecutorService executor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2,
        new BasicThreadFactory.Builder().namingPattern("common-schedule-pool-%d").daemon(true).build());
*/
    public void submit(Runnable task) {
        //executor.submit(task);
    }
}
