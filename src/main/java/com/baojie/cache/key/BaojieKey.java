package com.baojie.cache.key;

import com.baojie.cache.info.SourceDetail;

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
    // 有多少持有者
    private final AtomicLong holders = new AtomicLong(0);
    // 预计的超时时间
    private final AtomicLong expire = new AtomicLong(System.currentTimeMillis());
    // 可以外部调用方法进行设置
    private volatile TimeUnit unit = TimeUnit.MINUTES;
    // 可以外部调用方法进行设置
    private volatile long appendTime = 3;
    private volatile long inCachedTime = System.currentTimeMillis();

    protected final String key;
    protected final T info;

    // 可以增加其他的需要管控的属性进来
    protected BaojieKey(String key, T info) {
        if (null == key) {
            throw new NullPointerException("key not be null");
        } else if (null == info) {
            throw new NullPointerException("info not be null");
        } else {
            this.key = key;
            this.info = info;
            baseTimeOut();
        }
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

    // acquire 与 release 必须联合使用
    public final boolean acquire() {
        final ReentrantReadWriteLock.ReadLock read = readLock;
        read.lock();
        try {
            holders.incrementAndGet();
            long base = TimeUnit.MILLISECONDS.convert(appendTime, unit);
            long rand = makeRandom();
            long exp = expire.get();
            long now = System.currentTimeMillis();
            // 防止初始化 key 很长时间后在使用
            // 造成刚放入缓存的资源可能立即被清理
            if (exp <= now) {
                exp = now;
            }
            // append 基础 + append 随机
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
            // 以 0 为边界
            if (test > 0) {
                holders.decrementAndGet();
            }
        } finally {
            read.unlock();
        }
    }

    public final long existMillis() {
        long now = System.currentTimeMillis();
        long exist = now - inCachedTime;
        if (exist <= 0) {
            return 0;
        } else {
            return exist;
        }
    }

    public final long getInCachedTime() {
        return inCachedTime;
    }

    public final void setInCachedTime(long inCachedTime) {
        if (inCachedTime > this.inCachedTime) {
            this.inCachedTime = inCachedTime;
        }
    }

    // 探测正在使用资源的个数
    public final long getHolders() {
        return holders.get();
    }

    // 探测资源的过期时间
    public final long getExpire() {
        return expire.get();
    }

    // 两个必须联合调用
    public final boolean touch() {
        final ReentrantReadWriteLock.WriteLock write = writeLock;
        return write.tryLock();
    }

    // 两个必须联合调用
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
