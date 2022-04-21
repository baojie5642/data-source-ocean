package com.baojie.cache.util;

public class BaojieTGroup {

    private static volatile BaojieTGroup group;

    private BaojieTGroup() {

    }

    public static final BaojieTGroup getInstance() {
        if (null != group) {
            return group;
        } else {
            synchronized (BaojieTGroup.class) {
                if (null == group) {
                    group = new BaojieTGroup();
                }
            }
        }
        return group;
    }

    public final ThreadGroup group() {
        ThreadGroup tg = null;
        SecurityManager sm = System.getSecurityManager();
        if (null != sm) {
            tg = sm.getThreadGroup();
        } else {
            tg = Thread.currentThread().getThreadGroup();
        }
        if (null == tg) {
            throw new NullPointerException("ThreadGroup get from Main(JVM) must not be null");
        }
        return tg;
    }

}
