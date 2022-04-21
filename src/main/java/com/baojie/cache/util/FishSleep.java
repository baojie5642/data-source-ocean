package com.baojie.cache.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class FishSleep {

    private FishSleep() {

    }

    public static final void sleep(long time, TimeUnit unit) throws InterruptedException {
        if (FishSleep.checkAccess(time, unit)) {
            unit.sleep(time);
        } else {
            Thread.yield();
        }
    }

    public static final void sleepIgnore(long time, TimeUnit unit) {
        try {
            FishSleep.sleep(time, unit);
        } catch (InterruptedException e) {

        }
    }

    public static final void park(long time, TimeUnit unit) {
        if (FishSleep.checkAccess(time, unit)) {
            LockSupport.parkNanos(FishSleep.convertNanos(time, unit));
        } else {
            Thread.yield();
        }
    }

    public static final long convertNanos(long time, TimeUnit unit) {
        if (null == unit) {
            return 0;
        }
        if (time <= 0) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.convert(time, unit);
    }

    private static final boolean checkAccess(long time, TimeUnit unit) {
        if (time <= 0) {
            return false;
        }
        if (null == unit) {
            return false;
        } else {
            return true;
        }
    }

    // 确保 LockSupport 加载无问题
    static {
        Class<?> load = LockSupport.class;
        if (null == load) {
            throw new Error("load java LockSupport.class failed");
        }
    }

}
