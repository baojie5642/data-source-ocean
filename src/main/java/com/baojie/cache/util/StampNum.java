package com.baojie.cache.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

public class StampNum {

    private static final LongAdder tail = new LongAdder();
    private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    private static volatile StampNum instance;

    private StampNum() {

    }

    public static final StampNum getInstance() {
        StampNum copy = instance;
        if (null != copy) {
            return copy;
        } else {
            synchronized (StampNum.class) {
                copy = instance;
                if (null == copy) {
                    copy = instance = new StampNum();
                }
            }
        }
        return copy;
    }

    public final String keyNum() {
        long tid = Thread.currentThread().getId();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final StringBuilder buf = new StringBuilder(128);
        long now = System.currentTimeMillis();
        buf.append(now);
        buf.append("-");
        for (int i = 0; i < 6; i++) {
            buf.append(ID_CHARACTERS[random.nextInt(62)]);
        }
        buf.append("-");
        buf.append(tid);
        buf.append("-");
        tail.increment();
        buf.append(tail.sum());
        return buf.toString();
    }

}
