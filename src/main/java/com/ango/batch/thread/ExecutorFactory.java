package com.ango.batch.thread;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorFactory
{
    public static Executor getInstance(int threads, String threadNamePrefix)
    {
        return Executors.newFixedThreadPool(threads, new CustomThreadFactory(threadNamePrefix));
    }

    private static class CustomThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(0);
        private final String namePrefix;

        public CustomThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(namePrefix + "-" + threadNumber.getAndIncrement());
            return thread;
        }
    }
}
