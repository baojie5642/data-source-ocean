package com.baojie.cache.key;

import com.baojie.cache.info.SourceDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaojieKey<T extends SourceDetail> implements Key<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final long SEC_5 = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
    public static final long SEC_10 = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
    public static final long SEC_15 = TimeUnit.MILLISECONDS.convert(15, TimeUnit.SECONDS);
    public static final long SEC_25 = TimeUnit.MILLISECONDS.convert(25, TimeUnit.SECONDS);
    public static final long SEC_35 = TimeUnit.MILLISECONDS.convert(35, TimeUnit.SECONDS);
    public static final long SEC_45 = TimeUnit.MILLISECONDS.convert(45, TimeUnit.SECONDS);

    public static final long MIN_1_M = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
    public static final long MIN_3_M = TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES);
    public static final long MIN_5_M = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
    public static final long MIN_15_M = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);
    public static final long MIN_30_M = TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);
    public static final long MIN_45_M = TimeUnit.MILLISECONDS.convert(45, TimeUnit.MINUTES);
    public static final long MIN_60_M = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    public static final long HOUR_6 = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
    public static final long HOUR_8 = TimeUnit.MILLISECONDS.convert(8, TimeUnit.HOURS);
    public static final long HALF_DAY = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);

    public static final long ONE_DAY = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    public static final long TWO_DAY = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
    public static final long THREE_DAY = TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS);

    public static final long SEVEN_DAY = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);

    private final ReentrantReadWriteLock mainLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = mainLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = mainLock.writeLock();

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    // 统计总共地使用次数
    private final AtomicLong usedTimes = new AtomicLong(0);
    // 有多少持有者
    private final AtomicStampedReference<Integer> holders = new AtomicStampedReference(0, 0);
    // 预计的超时时间,不准确
    private final AtomicLong expire = new AtomicLong(System.currentTimeMillis());
    // 可以外部调用方法进行设置
    private volatile TimeUnit unit = TimeUnit.SECONDS;
    // 可以外部调用方法进行设置
    private volatile long appendTime = 15;
    // 记录什么时候放入缓存
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

    // 30 分钟基础时间
    private final long baseTimeOut() {
        final ReentrantReadWriteLock.WriteLock write = writeLock;
        write.lock();
        try {
            long now = System.currentTimeMillis();
            long temp = now + SEC_5;
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
            incrementHolders();
            usedTimes.incrementAndGet();
            // 需要添加的 15 sec 基础时间
            long base = TimeUnit.MILLISECONDS.convert(1, unit);
            // 需要添加的 以 15 sec 为基准 的随机时间
            long rand = makeRandom();
            // 可能存在少计算三分钟的情况
            // 但是并不影响实际的功能
            long exp = expire.get();
            // append 基础 + append 随机
            long append = exp + base + rand;
            expire.set(append);
            return true;
        } finally {
            read.unlock();
        }
    }

    private final void incrementHolders() {
        for (; ; ) {
            int current = holders.getReference().intValue();
            int stamp = holders.getStamp();
            if (holders.compareAndSet(current, current + 1, stamp, stamp + 1)) {
                break;
            }
        }
    }

    private final void decrementHolders() {
        for (; ; ) {
            int current = holders.getReference().intValue();
            if (current <= 0) {
                break;
            }
            int stamp = holders.getStamp();
            if (holders.compareAndSet(current, current - 1, stamp, stamp + 1)) {
                break;
            }
        }
    }

    // 以 15 sec 为基准 的随机时间
    private final long makeRandom() {
        final TimeUnit un = unit;
        final long app = 1;
        long ran = random.nextLong(TimeUnit.MILLISECONDS.convert(app, un)) + 1;
        return ran;
    }

    public final void release() {
        final ReentrantReadWriteLock.ReadLock read = readLock;
        read.lock();
        try {
            // 不需要对 延迟清理时间 进行进行更新
            long test = holders.getReference().intValue();
            // 以 0 为边界
            if (test > 0) {
                decrementHolders();
            }
        } finally {
            read.unlock();
        }
    }

    // 已经存在于cache中的时间
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
    public final int getHolders() {
        return holders.getReference().intValue();
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
        write.unlock();
        return true;
    }

    public final long hasUsedTimes() {
        return usedTimes.get();
    }

    @Override
    public final boolean equals(Object comp) {
        if (this == comp) {
            return true;
        }
        if (null == comp) {
            return false;
        }
        if (comp instanceof LocalKey) {
            LocalKey key = LocalKey.class.cast(comp);
            if (getKey().equals(key.getKey())) {
                return true;
            } else {
                return false;
            }
        } else if (comp instanceof BaojieKey) {
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
                "expire=" + expire +
                ", inCachedTime=" + inCachedTime +
                ", hasUsedTimes=" + usedTimes +
                ", key='" + key + '\'' +
                ", info=" + info +
                '}';
    }

}
