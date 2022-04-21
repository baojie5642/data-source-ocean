package com.baojie.cache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

public class BaojieUncaught implements UncaughtExceptionHandler {

    private final static Logger log = LoggerFactory.getLogger(BaojieUncaught.class);

    private static volatile BaojieUncaught instance;

    private BaojieUncaught() {

    }

    public static final BaojieUncaught getInstance() {
        if (null != instance) {
            return instance;
        } else {
            synchronized (BaojieUncaught.class) {
                if (null == instance) {
                    instance = new BaojieUncaught();
                }
            }
        }
        return instance;
    }

    @Override
    public final void uncaughtException(Thread t, Throwable e) {
        if (null == t) {
            if (null != e) {
                log.error(nameId(null) + "err msg=" + e.getMessage(), e);
            } else {
                log.error("thread null, throwable null" + nameId(null));
            }
        } else {
            deal(t, e);
        }
    }

    private void deal(Thread t, Throwable e) {
        toInterrupt(t);
        if (null == e) {
            log.error("throwable null" + nameId(t));
        } else {
            log.error(nameId(t) + "err msg=" + e.getMessage(), e);
        }
    }

    private String nameId(Thread t) {
        StringBuilder builder = new StringBuilder(128);
        if (null == t) {
            builder.append(", thread null, concurrent thread name=");
            String name = Thread.currentThread().getName();
            builder.append(name);
            builder.append(", concurrent id=");
            long id = Thread.currentThread().getId();
            builder.append(id);
        } else {
            builder.append(", thread name=");
            String name = t.getName();
            builder.append(name);
            builder.append(", thread id=");
            long id = t.getId();
            builder.append(id);
        }
        builder.append(", ");
        return builder.toString();
    }

    private void toInterrupt(Thread t) {
        try {
            t.interrupt();
        } finally {
            alwaysInterrupt(t);
        }
    }

    private void alwaysInterrupt(Thread t) {
        if (!t.isInterrupted()) {
            t.interrupt();
        }
        if (t.isAlive()) {
            t.interrupt();
        }
    }

}
