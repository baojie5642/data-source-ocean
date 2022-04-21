package com.baojie.cache.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BaojieFactory implements ThreadFactory {

    private final AtomicInteger tnum = new AtomicInteger(0);
    private final BaojieUncaught uncaught;
    private final ThreadGroup group;
    private final String fname;
    private final boolean daemon;

    private BaojieFactory(String fname, boolean daemon) {
        this.fname = fname;
        this.daemon = daemon;
        this.group = BaojieTGroup.getInstance().group();
        this.uncaught = BaojieUncaught.getInstance();
    }

    public static final BaojieFactory create(String fname, boolean daemon) {
        if (null == fname) {
            throw new NullPointerException();
        } else {
            return new BaojieFactory(fname, daemon);
        }
    }

    @Override
    public Thread newThread(final Runnable r) {
        String name = buildName();
        final Thread thread = new Thread(group, r, name);
        setProperties(thread);
        return thread;
    }

    private String buildName() {
        StringBuilder builder = new StringBuilder(128);
        builder.append("_thread name:");
        builder.append(fname);
        builder.append("-");
        builder.append(tnum.getAndIncrement());
        builder.append("_");
        return builder.toString();
    }

    private void setProperties(final Thread thread) {
        setDaemon(thread);
        setPriority(thread);
        thread.setUncaughtExceptionHandler(uncaught);
    }

    private void setDaemon(final Thread thread) {
        thread.setDaemon(daemon);
    }

    private void setPriority(final Thread thread) {
        if (thread.getPriority() != Thread.NORM_PRIORITY) {
            thread.setPriority(Thread.NORM_PRIORITY);
        }
    }

}
