package com.baojie.cache.key;

import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.util.StampNum;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaojieKey<T extends SourceDetail> implements Key<T> {

    public static final long SEC_10 = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
    public static final long SEC_45 = TimeUnit.MILLISECONDS.convert(45, TimeUnit.SECONDS);

    public static final long MIN_1_M = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
    public static final long MIN_3_M = TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES);
    public static final long MIN_5_M = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
    public static final long MIN_15_M = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);
    public static final long MIN_30_M = TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
    public static final long MIN_45_M = TimeUnit.MILLISECONDS.convert(45, TimeUnit.MINUTES);
    public static final long MIN_60_M = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    public static final long HOUR_6 = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
    public static final long HALF_DAY = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);

    public static final long ONE_DAY = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    public static final long TWO_DAY = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
    public static final long THREE_DAY = TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS);

    public static final long SEVEN_DAY = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);

    private final ReentrantReadWriteLock mainLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = mainLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = mainLock.writeLock();

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    // ??????????????????
    private final AtomicLong holders = new AtomicLong(0);
    // ?????????????????????
    private final AtomicLong expire = new AtomicLong(System.currentTimeMillis());
    // ????????????????????????????????????
    private volatile TimeUnit unit = TimeUnit.MINUTES;
    // ????????????????????????????????????
    private volatile long appendTime = 3;

    protected final String key;
    protected final T info;
    protected final StampNum stamp = StampNum.getInstance();

    // ????????????????????????????????????????????????
    protected BaojieKey(String key, T info) {
        if (null == key) {
            throw new NullPointerException("key not be null");
        } else {
            this.key = key;
            this.info = info;
            baseTimeOut();
        }
    }

    protected BaojieKey() {
        this.key = stamp.keyNum();
        this.info = null;
    }

    private final long baseTimeOut() {
        final ReentrantReadWriteLock.WriteLock write = writeLock;
        write.lock();
        try {
            long now = System.currentTimeMillis();
            long temp = now + MIN_45_M;
            expire.set(temp);
            return temp;
        } finally {
            write.unlock();
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public T info() {
        return info;
    }

    // acquire ??? release ??????????????????
    public final boolean acquire() {
        final ReentrantReadWriteLock.ReadLock read = readLock;
        read.lock();
        try {
            holders.incrementAndGet();
            long base = TimeUnit.MILLISECONDS.convert(appendTime, unit);
            long rand = makeRandom();
            long exp = expire.get();
            long now = System.currentTimeMillis();
            // ??????????????? key ????????????????????????
            // ???????????????????????????????????????????????????
            if (exp <= now) {
                exp = now;
            }
            // append ?????? + append ??????
            long append = exp + base + rand;
            expire.set(append);
            return true;
        } finally {
            read.unlock();
        }
    }

    private final long makeRandom() {
        final TimeUnit un = unit;
        final long app = appendTime;
        long ran = random.nextLong(TimeUnit.MILLISECONDS.convert(app, un)) + 1;
        return ran;
    }

    public final void release() {
        final ReentrantReadWriteLock.ReadLock read = readLock;
        read.lock();
        try {
            long test = holders.get();
            // ??? 0 ?????????
            if (test > 0) {
                holders.decrementAndGet();
            }
        } finally {
            read.unlock();
        }
    }

    // ?????????????????????????????????
    public final long getHolders() {
        return holders.get();
    }

    // ???????????????????????????
    public final long getExpire() {
        return expire.get();
    }

    // ????????????????????????
    public final boolean touch() {
        final ReentrantReadWriteLock.WriteLock write = writeLock;
        return write.tryLock();
    }

    // ????????????????????????
    public final boolean untouch() {
        final ReentrantReadWriteLock.WriteLock write = writeLock;
        return write.tryLock();
    }

    @Override
    public final boolean equals(Object comp) {
        if (this == comp) {
            return true;
        }
        if (null == comp) {
            return false;
        }
        if (comp instanceof BaojieKey) {
            BaojieKey key = BaojieKey.class.cast(comp);
            if (getKey().equals(key.getKey())) {
                return true;
            } else {
                return false;
            }
        } else {
            return super.equals(comp);
        }
    }

    @Override
    public final int hashCode() {
        int result = 17;
        int innerHash = key.hashCode();
        result = 31 * result + innerHash;
        return result;
    }

    @Override
    public String toString() {
        return "BaojieKey{" +
                "key='" + key + '\'' +
                '}';
    }

}
