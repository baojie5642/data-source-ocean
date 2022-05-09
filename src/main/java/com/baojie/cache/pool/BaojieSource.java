package com.baojie.cache.pool;

import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.key.BaojieKey;
import com.baojie.cache.key.DelayKey;
import com.baojie.cache.key.LocalKey;
import com.baojie.cache.util.BaojieFactory;
import com.baojie.cache.util.BaojieTPool;
import com.baojie.cache.util.FishSleep;
import com.baojie.cache.value.BaojieValue;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaojieSource<K extends BaojieKey, V extends BaojieValue> implements Pool<K, V> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    // 可以考虑使用并发的队列
    // 使用周期的探测
    // 取出 -> 检测 -> 是否再放回队列
    private final DelayQueue<DelayKey<K>> delays = new DelayQueue<>();

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    // 缓存的同步控制主要处理新增缓存与延迟清理的冲突
    private final ReentrantReadWriteLock mainLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = mainLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = mainLock.writeLock();

    private final ConcurrentHashMap<K, V> cache;
    // 专门负责关闭的队列
    private final LinkedBlockingQueue<V> toCloses;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final List<Future<?>> wfs = new ArrayList<>(64);
    private final List<Future<?>> cfs = new ArrayList<>(64);
    private final List<Future<?>> mfs = new ArrayList<>(64);
    private final AtomicStampedReference<Integer> size = new AtomicStampedReference<>(0, 0);
    // 负责 观察 过期时间的工作池
    private final BaojieTPool wps;
    // 负责 关闭 数据源连接池的工作池
    private final BaojieTPool cps;
    // 负责 监控 所有数据源链接池的工作池
    private final BaojieTPool mps;
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
            // 防止 map 的扩容
            int temp = (((contains / 3) + 1) * 4) + 7;
            this.cache = new ConcurrentHashMap<>(temp);
            this.toCloses = new LinkedBlockingQueue<>(contains);
            this.contains = contains;
            this.name = name;
            this.wps = createPool(name + "-watch");
            this.cps = createPool(name + "-close");
            this.mps = createPool(name + "-monitor");
            startCloser();
            startWatcher();
            startMonitor();
        }
    }

    private final BaojieTPool createPool(String name) {
        return new BaojieTPool(4, 32, 60, TimeUnit.SECONDS, new SynchronousQueue<>(), BaojieFactory.create(name, true));
    }

    // 更多的关闭线程
    // 防止关闭时候造成阻塞
    private final void startCloser() {
        for (int i = 0; i < 4; i++) {
            Cleaner cleaner = new Cleaner();
            Future<?> fu = cps.submit(cleaner);
            cfs.add(fu);
        }
    }

    // 与上面一比一的比例创建
    // 但是开的太多却又影响着延迟队列中数据的处理速度
    // 因为有锁的竞争
    // 所以还是开一个比较好
    private final void startWatcher() {
        for (int i = 0; i < 1; i++) {
            Watcher cleaner = new Watcher();
            Future<?> fu = wps.submit(cleaner);
            wfs.add(fu);
        }
    }

    // 多数据源缓存框架的监控线程可以开的少一点
    // 一个线程就好了
    private final void startMonitor() {
        for (int i = 0; i < 1; i++) {
            Monitor cleaner = new Monitor();
            Future<?> fu = mps.submit(cleaner);
            mfs.add(fu);
        }
    }

    @Override
    public V getCached(K key) {
        if (stopped()) {
            throw closeException();
        }
        if (null == key) {
            throw nullKeyException();
        } else {
            // 此处加上为了防止延迟清理定时器将get出来的数据源关闭
            // 所以就不需要再key中标记是否已经损坏了
            final ReentrantReadWriteLock.ReadLock read = readLock;
            read.lock();
            try {
                V old = null;
                try {
                    old = cache.get(key);
                } finally {
                    if (null != old) {
                        old.acquire();
                    }
                }
                return old;
            } finally {
                read.unlock();
            }
        }
    }

    private final RuntimeException closeException() {
        return new IllegalStateException("websql source ocean closed ...");
    }

    private final RuntimeException nullKeyException() {
        return new NullPointerException("key can not be null ...");
    }

    private final RuntimeException nullValueException() {
        return new NullPointerException("value can not be null ...");
    }

    // 此处的 key 中的信息必须是完整的
    @Override
    public V acquire(K key) {
        if (stopped()) {
            throw closeException();
        }
        if (null == key) {
            throw nullKeyException();
        } else {
            // 此处加上为了防止延迟清理定时器将get出来的数据源关闭
            // 所以就不需要再key中标记是否已经损坏了
            final ReentrantReadWriteLock.ReadLock read = readLock;
            read.lock();
            try {
                V old = cache.get(key);
                // 返回 已经缓存的 数据源
                if (null != old) {
                    return anCachedOne(old);
                } else {
                    // 返回 一个新构建的 数据源
                    return makeAnNewer(key);
                }
            } finally {
                read.unlock();
            }
        }
    }

    private final V anCachedOne(V old) {
        old.acquire();
        return old;
    }

    private final V makeAnNewer(K key) {
        V value = buildSource(key);
        if (null == value) {
            throw nullValueException();
        } else {
            boolean canPutIn = sizeTest();
            // 容量达到上限
            // 不能放入缓存
            if (canPutIn) {
                return mayPutInCache(key, value);
            } else {
                return canNotPutInCache(value);
            }
        }
    }

    private final V canNotPutInCache(V value) {
        try {
            value.setNotInCache();
        } finally {
            value.acquire();
        }
        return value;
    }

    // 此处会出现并发问题
    // 也就是刚放入缓存并且一个线程已经从cache中拿到
    // 但是马上被当前线程清理掉
    // 这种情况是在放入延迟队列失败的情况下发生的
    // 但是问题不大
    private final V mayPutInCache(K key, V value) {
        V old = cache.putIfAbsent(key, value);
        if (null == old) {
            return successPutInCache(key, value);
        } else {
            return occurCurrentPut(old, value);
        }
    }

    private final V successPutInCache(K key, V value) {
        long millis = firstPutInDelay(key);
        DelayKey<K> dk = new DelayKey<>(key, millis, TimeUnit.MILLISECONDS);
        boolean suc = false;
        try {
            // 成功放入延迟队列
            suc = offerDelayQueue(dk);
        } finally {
            // 放入延迟队列失败
            // 删除缓存并且设置未放入缓存标记
            // 计数器自减
            try {
                if (suc) {
                    value.setInCache();
                    // 在 key 中标记缓存放入的时间
                    key.setInCachedTime(System.currentTimeMillis());
                } else {
                    // 删除缓存
                    cache.remove(key);
                    // 计数器自减
                    decrementSize();
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
            // 需要将新创建的关闭掉
            if (!offerCloseQueue(newer)) {
                closeValue(newer);
            }
        }
    }

    private final long firstPutInDelay(K k) {
        long expire = k.getExpire();
        long delay = expire - System.currentTimeMillis();
        if (delay <= 0) {
            delay = 300;
        }
        return millisInDelayQueue(delay);
    }

    // 输入参数 被 加了 15 秒
    private final long millisInDelayQueue(long base) {
        // 15 秒基础 + 15 秒随机
        long ran_0 = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
        // 最后还加了 300 毫秒
        long ran_1 = ran_0 + random.nextLong(ran_0) + 300;
        // base 基础 + 30.3 秒随机(大概的值)
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
            final ReentrantReadWriteLock.ReadLock read = readLock;
            read.lock();
            try {
                try {
                    v.release();
                } finally {
                    checkCacheStatus(v);
                }
            } finally {
                read.unlock();
            }
        }
        return true;
    }

    private final void checkCacheStatus(V v) {
        boolean isCached = v.isCached();
        // 如果没有缓存在cache中那么需要关闭
        if (!isCached) {
            BaojieKey key = shiftToKey(v);
            try {
                cache.remove(key);
            } finally {
                if (!offerCloseQueue(v)) {
                    closeValue(v);
                }
            }
        }
    }

    private final BaojieKey<SourceDetail> shiftToKey(V v) {
        String key = v.getKey().getKey();
        return new LocalKey<>(key, new SourceDetail());
    }

    private final void closeValue(V v) {
        if (null != v) {
            try {
                v.close();
            } catch (Throwable t) {

            }
        }
    }

    // 此处删除完毕后需要在延迟队列中检测key是否还在cache中缓存着
    public final boolean remove(K key) {
        if (null == key) {
            return false;
        } else {
            V old = cache.remove(key);
            if (null == old) {
                return false;
            } else {
                try {
                    old.setNotInCache();
                    // 没有成功直接close
                    if (!offerCloseQueue(old)) {
                        closeValue(old);
                    }
                    return true;
                } finally {
                    decrementSize();
                }
            }
        }
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
            try {
                stopCloser();
            } finally {
                stopMonitor();
            }
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

    private final void stopMonitor() {
        try {
            mps.cancel(mfs, mps);
        } finally {
            mps.shutdown(mps);
        }
    }

    private final void closeCache() {
        Iterator<V> iterator = cache.values().iterator();
        while (iterator.hasNext()) {
            V value = iterator.next();
            try {
                try {
                    closeValue(value);
                } finally {
                    iterator.remove();
                }
            } catch (Throwable t) {

            }
        }
    }

    private final void closeQueue() {
        for (; ; ) {
            V value = toCloses.poll();
            if (null != value) {
                closeValue(value);
            } else {
                break;
            }
        }
    }

    public abstract V buildSource(K k);

    private final boolean stopped() {
        if (stop.get()) {
            return true;
        } else {
            return false;
        }
    }

    // 这种探测不需要获取key上面的锁,仅仅获取到cache上面的锁即可
    // 因为key的时间是递增的
    private final boolean longTimeNoUse(K k) {
        long expire = k.getExpire();
        long now = System.currentTimeMillis();
        long temp = now - expire;
        long dura = Math.abs(temp);
        // 大于 或者 小于 8小时 都需要关闭
        if (dura >= BaojieKey.HOUR_6) {
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
            String name = Thread.currentThread().getName();
            for (; ; ) {
                if (stopped()) {
                    break;
                } else {
                    watch(name);
                }
            }
        }

        private void watch(String name) {
            DelayKey<K> delay = getDelayKey();
            if (null != delay) {
                K k = delay.getKey();
                if (null != k) {
                    keyNotNull(k);
                } else {
                    // do not know do what
                    // can not be happened
                    log.error("unknow error, get key from delay null ...");
                }
            } else {
                long now = System.currentTimeMillis();
                int size = delays.size();
                log.info("i am delay watcher, name=" + name + ", delay queue size=" + size + ", is working now ...");
                // 优先响应停止信号
                if (!stopped()) {
                    fishSleep();
                }
            }
        }

        private void fishSleep() {
            FishSleep.park(300, TimeUnit.MILLISECONDS);
        }

        private void keyNotNull(K k) {
            if (checkPass(k)) {
                rightTypeKey(k);
            } else {
                // log error
                // do not offer again
                String cn = k.getClass().getName();
                log.error("unknow class key, class name = " + cn);
            }
        }

        private boolean checkPass(K k) {
            if (k instanceof LocalKey) {
                return true;
            } else if (k instanceof BaojieKey) {
                return true;
            } else {
                return false;
            }
        }

        private void rightTypeKey(K k) {
            // 应该先上 外部 锁
            /** 使用 tryLock 防止上面
             * {@link #acquire(BaojieKey)}
             * 方法中获取两把锁的顺序不同从而造成死锁
             * 不能使用 for 循环来 tryLock
             * 会出现一致性问题
             **/
            final ReentrantReadWriteLock.WriteLock write = writeLock;
            if (write.tryLock()) {
                try {
                    dealTheKey(k);
                } finally {
                    write.unlock();
                }
            } else {
                // 外部大锁没锁住
                // 直接关闭了会造成强制关闭
                // 但是随着用户的继续使用还会继续创建
                rePutDelay(k, true);
            }
        }

        private void dealTheKey(K k) {
            // 说明没有人试图去使用此缓存
            // 这样对key的处理才是安全的
            if (k.touch()) {
                try {
                    int holders = k.getHolders();
                    // can be <0 ?
                    if (holders <= 0) {
                        noUserHolders(k);
                    } else {
                        // 根据时间判断
                        chargeWithExpire(k);
                    }
                } finally {
                    k.untouch();
                }
            } else {
                rePutDelay(k, true);
            }
        }

        private void noUserHolders(K k) {
            long expire = k.getExpire();
            boolean noUse = longTimeNoUse(k);
            if (noUse) {
                // 如果长时间不用直接close
                cacheRemove(k);
            } else {
                long now = System.currentTimeMillis();
                if (now >= expire) {
                    cacheRemove(k);
                } else {
                    rePutDelay(k, false);
                }
            }
        }

        private void chargeWithExpire(K k) {
            boolean noUse = longTimeNoUse(k);
            if (noUse) {
                cacheRemove(k);
            } else {
                rePutDelay(k, false);
            }
        }

        private void rePutDelay(K k, boolean checkExpire) {
            // 是否检测缓存时间过长
            if (checkExpire) {
                boolean noUse = longTimeNoUse(k);
                if (noUse) {
                    cacheRemove(k);
                } else {
                    onlyRePutDelay(k);
                }
            } else {
                onlyRePutDelay(k);
            }
        }

        // 此方法改进,实现快速的清理
        // 也就是 re put 的时间初始化成固定的30秒就好了
        private void onlyRePutDelay(K k) {
            Object test = cache.get(k);
            // 只有在缓存中的才进行二次延迟
            if (null != test) {
                long rep = millisInDelayQueue(0L);
                DelayKey<K> key = new DelayKey<>(k, rep, TimeUnit.MILLISECONDS);
                // 修复代码错误,原来是没有 取反
                if (!offerDelayQueue(key)) {
                    cacheRemove(k);
                    log.error("re-put delay fail, key = " + k);
                }
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
                        BaojieKey key = old.getKey();
                        try {
                            closeValue(old);
                        } finally {
                            log.error("put value into closeQueue fail, key = " + key + ", then close in current thread ...");
                        }
                    }
                }
            }
        }

    }

    private final boolean offerCloseQueue(V close) {
        for (int i = 0; i < 10; i++) {
            boolean suc = false;
            try {
                suc = toCloses.offer(close, 100, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                suc = false;
            }
            if (suc) {
                return true;
            } else {
                continue;
            }
        }
        return false;
    }

    private final boolean offerDelayQueue(DelayKey<K> delayKey) {
        for (int i = 0; i < 6; i++) {
            boolean suc = false;
            try {
                suc = delays.offer(delayKey, 200, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                suc = false;
            }
            if (suc) {
                return true;
            } else {
                continue;
            }
        }
        return false;
    }


    private final DelayKey<K> getDelayKey() {
        try {
            return delays.poll(1, TimeUnit.SECONDS);
        } catch (Throwable t) {

        }
        return null;
    }

    private final V getCloseFromQueue() {
        try {
            return toCloses.poll(1, TimeUnit.SECONDS);
        } catch (Throwable t) {

        }
        return null;
    }

    private final class Cleaner implements Runnable {

        public Cleaner() {

        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            for (; ; ) {
                if (stopped()) {
                    closeQueue();
                    break;
                } else {
                    V v = getCloseFromQueue();
                    if (null != v) {
                        BaojieKey key = v.getKey();
                        try {
                            closeValue(v);
                        } finally {
                            log.info("i am closer, name=" + name + ", has close one hikariCP source, info = " + key);
                        }
                    } else {
                        log.info("i am closer, name=" + name + ", is working now ...");
                    }
                }
            }
        }

    }

    private final class Monitor implements Runnable {

        public Monitor() {

        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            for (; ; ) {
                if (stopped()) {
                    break;
                } else {
                    try {
                        int count = cache.size();
                        if (count <= 0) {
                            log.info("i am monitor, name=" + name + ", no data source cached in websql ocean pool ...");
                        } else {
                            try {
                                Iterator<Map.Entry<K, V>> iterator = entryIterator();
                                visitEntrys(iterator, count);
                            } finally {
                                log.info("i am monitor, name=" + name + ", is working now ...");
                            }
                        }
                    } finally {
                        if (!stopped()) {
                            FishSleep.park(1, TimeUnit.SECONDS);
                        }
                    }
                }
            }
        }

        private Iterator<Map.Entry<K, V>> entryIterator() {
            return cache.entrySet().iterator();
        }

        private void visitEntrys(Iterator<Map.Entry<K, V>> iterator, int count) {
            while (iterator.hasNext()) {
                Map.Entry<K, V> entry = iterator.next();
                lookSource(entry, count);
                if (stopped()) {
                    break;
                }
            }
        }

        private void lookSource(Map.Entry<K, V> entry, int count) {
            V value = entry.getValue();
            Object source = value.getSource();
            if (null != source) {
                checkSource(entry, source, count);
            }
        }

        private void checkSource(Map.Entry<K, V> entry, Object source, int count) {
            if (source instanceof HikariDataSource) {
                HikariDataSource hikari = HikariDataSource.class.cast(source);
                dealHikariCP(entry, hikari, count);
            } else {
                String cn = source.getClass().getName();
                log.error("unknow class of source, class name = " + cn);
            }
        }

        private void dealHikariCP(Map.Entry<K, V> entry, HikariDataSource hikari, int count) {
            K key = entry.getKey();
            SourceDetail detail = key.info();
            // 如果 关闭了 或者 不在运行清除缓存
            if (hikari.isClosed() || (!hikari.isRunning())) {
                cache.remove(key);
            } else {
                String domain = detail.getDomain();
                long exist = key.existMillis();
                long sec = TimeUnit.SECONDS.convert(exist, TimeUnit.MILLISECONDS);
                long min = TimeUnit.MINUTES.convert(exist, TimeUnit.MILLISECONDS);
                long expire = key.getExpire();
                long usedTimes = key.hasUsedTimes();

                Date cleanTime = new Date(expire);

                HikariPoolMXBean poolMXBean = hikari.getHikariPoolMXBean();
                String poolName = hikari.getPoolName();
                int total = poolMXBean.getTotalConnections();
                int active = poolMXBean.getActiveConnections();
                int idle = poolMXBean.getIdleConnections();
                int wait = poolMXBean.getThreadsAwaitingConnection();

                StringBuilder builder = new StringBuilder(512);
                builder.append("websql ocean size = ");
                builder.append(count);
                builder.append(", domain = ");
                builder.append(domain);
                builder.append(", has in cache = ");
                builder.append(sec);
                builder.append(" sec, min = ");
                builder.append(min);
                builder.append(" min, used times = ");
                builder.append(usedTimes);
                builder.append(", expire to close = ");
                builder.append(cleanTime);
                builder.append(", source pool name = ");
                builder.append(poolName);
                builder.append(", total conn = ");
                builder.append(total);
                builder.append(", active conn = ");
                builder.append(active);
                builder.append(", idle conn = ");
                builder.append(idle);
                builder.append(", wait to get conn threads = ");
                builder.append(wait);
                String info = builder.toString();
                log.info(info);
            }
        }

    }

}
