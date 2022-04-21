package com.baojie.cache.pool;

import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.key.BaojieKey;
import com.baojie.cache.key.DelayKey;
import com.baojie.cache.key.LocalKey;
import com.baojie.cache.util.BaojieFactory;
import com.baojie.cache.util.BaojieTPool;
import com.baojie.cache.util.FishSleep;
import com.baojie.cache.value.BaojieValue;
import com.baojie.cache.value.LocalValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaojieSource<K extends BaojieKey, V extends BaojieValue> implements Pool<K, V> {

    // 可以考虑使用并发的队列
    // 使用周期的探测
    // 取出 -> 检测 -> 是否再放回队列
    private final DelayQueue<DelayKey<K>> delays = new DelayQueue<>();
    // 专门负责关闭的队列

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    // 缓存的同步控制主要处理新增缓存与延迟清理的冲突
    private final ReentrantReadWriteLock mainLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = mainLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = mainLock.writeLock();

    private final ConcurrentHashMap<K, V> cache;
    private final LinkedBlockingQueue<V> toCloses;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final List<Future<?>> wfs = new ArrayList<>(64);
    private final List<Future<?>> cfs = new ArrayList<>(64);
    private final AtomicStampedReference<Integer> size = new AtomicStampedReference<>(0, 0);
    private final BaojieTPool wps;
    private final BaojieTPool cps;
    private final String name;
    private final int contains;
    private volatile int sleep = 180;

    protected BaojieSource(int contains, String name) {
        if (contains < 0) {
            throw new IllegalArgumentException();
        }
        if (null == name) {
            throw new NullPointerException();
        } else {
            int temp = (((contains / 3) + 1) * 4) + 7;
            this.cache = new ConcurrentHashMap<>(temp);
            this.toCloses = new LinkedBlockingQueue<>(contains);
            this.contains = contains;
            this.name = name;
            this.wps = createPool(name + "-watch");
            this.cps = createPool(name + "-close");
            startCloser();
            startWatcher();
        }
    }

    private final BaojieTPool createPool(String name) {
        return new BaojieTPool(4, 32, 60, TimeUnit.SECONDS, new SynchronousQueue<>(), BaojieFactory.create(name, true));
    }

    private final void startCloser() {
        for (int i = 0; i < 4; i++) {
            Cleaner cleaner = new Cleaner();
            Future<?> fu = cps.submit(cleaner);
            cfs.add(fu);
        }
    }

    private final void startWatcher() {
        for (int i = 0; i < 2; i++) {
            Watcher cleaner = new Watcher();
            Future<?> fu = wps.submit(cleaner);
            wfs.add(fu);
        }
    }

    @Override
    public V acquire(K key) {
        if (stopped()) {
            throw new IllegalStateException();
        }
        if (null == key) {
            throw new NullPointerException();
        } else {
            // 此处加上为了防止延迟清理定时器将get出来的数据源关闭
            // 所以就不需要再key中标记是否已经损坏了
            final ReentrantReadWriteLock.ReadLock read = readLock;
            read.lock();
            try {
                V old = cache.get(key);
                if (null != old) {
                    return anCachedOne(old);
                } else {
                    return makeAnNewer(key);
                }
            } finally {
                read.unlock();
            }
        }
    }

    private final V anCachedOne(V old) {
        // 使用前一定要获取许可
        old.acquire();
        return old;
    }

    private final V makeAnNewer(K key) {
        V value = buildSource(key);
        if (null == value) {
            throw new NullPointerException();
        } else {
            boolean canPutIn = sizeTest();
            // 容量达到上限
            // 不能放入缓存
            if (!canPutIn) {
                return canNotPutInCache(value);
            } else {
                return mayPutInCache(key, value);
            }
        }
    }

    private final V canNotPutInCache(V value) {
        // 设置不在缓存标记
        value.setNotInCache();
        value.acquire();
        return value;
    }

    private final V mayPutInCache(K key, V value) {
        V old = cache.putIfAbsent(key, value);
        if (null == old) {
            return successPutInCache(key, value);
        } else {
            return occurCurrentPut(old, value);
        }
    }

    private final V successPutInCache(K key, V value) {
        long millis = randomMillisDelay(key);
        DelayKey<K> dk = new DelayKey<>(key, millis, TimeUnit.MILLISECONDS);
        boolean suc = false;
        try {
            // 成功放入延迟队列
            suc = delays.offer(dk);
        } finally {
            // 放入延迟队列失败
            // 删除缓存并且设置未放入缓存标记
            // 计数器自减
            try {
                if (!suc) {
                    // 删除缓存
                    cache.remove(key);
                    // 计数器自减
                    decrementSize();
                    // 设置不在缓存标记
                    value.setNotInCache();
                }
            } finally {
                // 无论如何都要 acquire
                value.acquire();
            }
        }
        return value;
    }

    private final V occurCurrentPut(V oldCached, V newer) {
        try {
            // 出现并发放入
            // 计数器自减
            // 因为在探测计数器成功后
            // 已经自增所以要自减
            decrementSize();
            // acquire 的是已经缓存的
            // 然后将已经缓存的返回出去
            oldCached.acquire();
            return oldCached;
        } finally {
            try {
                newer.setNotInCache();
            } finally {
                // 需要将新创建的关闭掉
                newer.close();
            }
        }
    }

    private final long randomMillisDelay(K k) {
        long expire = k.getExpire();
        long delay = expire - System.currentTimeMillis();
        if (delay < 0) {
            delay = 300;
        }
        return millisInDelayQueue(delay);
    }

    private final long millisInDelayQueue(long base) {
        // 30 秒基础 + 30 秒随机
        long ran_0 = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        // 最后还加了 300 毫秒
        long ran_1 = ran_0 + random.nextLong(ran_0) + 300;
        // base 基础 + 30.3 秒随机
        return base + ran_1;
    }

    private final boolean sizeTest() {
        int sum = size.getReference().intValue();
        if (sum >= contains) {
            return false;
        } else {
            incrementSize();
            int temp = size.getReference().intValue();
            // 当缓存数量快达到上限时
            // 缓存会出现抖动问题
            if (temp > contains) {
                decrementSize();
                return false;
            } else {
                return true;
            }
        }
    }

    private final void incrementSize() {
        for (; ; ) {
            int current = size.getReference().intValue();
            int stamp = size.getStamp();
            if (size.compareAndSet(current, current + 1, stamp, stamp + 1)) {
                break;
            }
        }
    }

    private final void decrementSize() {
        for (; ; ) {
            int current = size.getReference().intValue();
            int stamp = size.getStamp();
            if (size.compareAndSet(current, current - 1, stamp, stamp + 1)) {
                break;
            }
        }
    }

    @Override
    public boolean release(V v) {
        if (null != v) {
            try {
                v.release();
            } finally {
                boolean isCached = v.isCached();
                // 如果发生阻塞
                // 阻塞的是外部调用线程
                if (!isCached) {
                    v.close();
                }
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (!stopped()) {
            final ReentrantReadWriteLock.WriteLock write = writeLock;
            write.lock();
            try {
                if (stop.compareAndSet(false, true)) {
                    try {
                        shutDown();
                    } finally {
                        closeSource();
                    }
                    cache.clear();
                    toCloses.clear();
                    delays.clear();
                }
            } finally {
                write.unlock();
            }
        }
    }

    private final void shutDown() {
        try {
            stopWatcher();
        } finally {
            stopCloser();
        }
    }

    private final void closeSource() {
        try {
            closeCache();
        } finally {
            closeQueue();
        }
    }

    private final void stopWatcher() {
        try {
            wps.cancel(wfs, wps);
        } finally {
            wps.shutdown(wps);
        }
    }

    private final void stopCloser() {
        try {
            cps.cancel(cfs, cps);
        } finally {
            cps.shutdown(cps);
        }
    }

    private final void closeCache() {
        Iterator<V> iterator = cache.values().iterator();
        while (iterator.hasNext()) {
            V value = iterator.next();
            value.close();
        }
    }

    private final void closeQueue() {
        for (; ; ) {
            V value = toCloses.poll();
            if (null != value) {
                value.close();
            } else {
                break;
            }
        }
    }

    public abstract V buildSource(K k);

    @Override
    public abstract V acquire(SourceDetail detail);

    private final boolean stopped() {
        if (stop.get()) {
            return true;
        } else {
            return false;
        }
    }

    private final boolean longTimeNoUse(K k) {
        long expire = k.getExpire();
        long now = System.currentTimeMillis();
        long temp = now - expire;
        long dura = Math.abs(temp);
        // 大于或者小于三天都需要关闭
        if (dura >= BaojieKey.SEVEN_DAY) {
            return true;
        } else {
            return false;
        }
    }

    private final class Watcher implements Runnable {

        public Watcher() {

        }

        @Override
        public void run() {
            for (; ; ) {
                if (stopped()) {
                    break;
                } else {
                    watch();
                }
            }
        }

        private void watch() {
            DelayKey<K> delay = getDelayKey();
            if (null != delay) {
                K k = delay.getKey();
                if (null != k) {
                    keyNotNull(k);
                } else {
                    // do not know do what
                    // can not be happened
                }
            } else {
                fishSleep();
            }
        }

        private void fishSleep() {
            FishSleep.park(300, TimeUnit.MILLISECONDS);
        }

        private void keyNotNull(K k) {
            if (k instanceof BaojieKey) {
                rightTypeKey(k);
            } else {
                // log error
                // do not offer again
            }
        }

        private void rightTypeKey(K k) {
            // 应该先上 外部 锁
            final ReentrantReadWriteLock.WriteLock write = writeLock;
            boolean lockCache = false;
            try {
                /** 使用 tryLock 防止上面
                 * {@link #acquire(BaojieKey)}
                 * 方法中获取两把锁的顺序不同从而造成死锁
                 * 不能使用 for 循环来 tryLock
                 * 会出现一致性问题
                 **/
                lockCache = write.tryLock();
                if (lockCache) {
                    dealTheKey(k);
                } else {
                    rePutDelay(k);
                }
            } finally {
                if (lockCache) {
                    write.unlock();
                }
            }
        }

        private void dealTheKey(K k) {
            boolean sucTouch = false;
            try {
                // 说明没有人试图去使用此缓存
                // 这样对key的处理才是安全的
                sucTouch = k.touch();
                if (sucTouch) {
                    long holders = k.getHolders();
                    // can be <0 ?
                    if (holders <= 0) {
                        noUserHolders(k);
                    } else {
                        // 根据时间判断
                        chargeWithExpire(k);
                    }
                } else {
                    rePutDelay(k);
                }
            } finally {
                if (sucTouch) {
                    k.untouch();
                }
            }
        }

        private void noUserHolders(K k) {
            long expire = k.getExpire();
            if (longTimeNoUse(k)) {
                // 如果长时间不用直接close
                // 不需要 touch
                cacheRemove(k);
            } else {
                long now = System.currentTimeMillis();
                if (now >= expire) {
                    cacheRemove(k);
                } else {
                    rePutDelay(k);
                }
            }
        }

        private void chargeWithExpire(K k) {
            boolean noUse = longTimeNoUse(k);
            if (noUse) {
                cacheRemove(k);
            } else {
                rePutDelay(k);
            }
        }

        private void rePutDelay(K k) {
            long expire = k.getExpire();
            long now = System.currentTimeMillis();
            long base = expire - now;
            if (base <= 0) {
                base = 300;
            }
            long rep = millisInDelayQueue(base);
            DelayKey<K> key = new DelayKey<>(k, rep, TimeUnit.MILLISECONDS);
            if (delays.offer(key)) {
                // print error
            }
        }

        private void cacheRemove(K k) {
            V old = null;
            try {
                old = cache.remove(k);
            } finally {
                closeAndDecrement(old);
            }
        }

        private void closeAndDecrement(V old) {
            if (null != old) {
                try {
                    decrementSize();
                } finally {
                    // 异步关闭
                    // 不影响清理线程执行
                    // 如果放入关闭队列不成功则当前线程关闭
                    if (!offerCloseQueue(old)) {
                        old.close();
                    }
                }
            }
        }

    }

    private final boolean offerCloseQueue(V close) {
        boolean suc = false;
        try {
            suc = toCloses.offer(close, 1, TimeUnit.SECONDS);
        } catch (Throwable t) {
            suc = false;
        }
        return suc;
    }


    private final DelayKey<K> getDelayKey() {
        try {
            return delays.poll(sleep, TimeUnit.SECONDS);
        } catch (Throwable t) {

        }
        return null;
    }

    private final V getCloseFromQueue() {
        try {
            return toCloses.poll(sleep, TimeUnit.SECONDS);
        } catch (Throwable t) {

        }
        return null;
    }

    private final class Cleaner implements Runnable {

        public Cleaner() {

        }

        @Override
        public void run() {
            for (; ; ) {
                if (stopped()) {
                    closeAllSource();
                    break;
                } else {
                    V v = getCloseFromQueue();
                    if (null != v) {
                        v.close();
                    }
                }
            }
        }

        private void closeAllSource() {
            for (; ; ) {
                V v = toCloses.poll();
                if (null != v) {
                    v.close();
                } else {
                    break;
                }
            }
        }

    }

}
