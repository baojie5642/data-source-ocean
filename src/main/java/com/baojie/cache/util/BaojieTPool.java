package com.baojie.cache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class BaojieTPool extends ThreadPoolExecutor {

    private final static Logger log = LoggerFactory.getLogger(BaojieTPool.class);

    public BaojieTPool(
            int core,
            int max,
            long keep,
            TimeUnit unit,
            BlockingQueue<Runnable> queue,
            ThreadFactory factory) {
        super(core, max, keep, unit, queue, factory);
    }

    public final void beforeExecute(Thread t, Runnable r) {
    }

    public final void afterExecute(Runnable r, Throwable t) {
        if (null == r) {
            return;
        } else {
            if (r instanceof FutureTask) {
                FutureTask<?> f = (FutureTask<?>) r;
                if (f.isDone() || f.isCancelled()) {
                    dealFuture(f, r, t);
                }
            } else {
                error(r, t);
            }
        }
    }

    private final void dealFuture(FutureTask<?> f, Runnable r, Throwable t) {
        Object suc = null;
        try {
            suc = f.get();
        } catch (Throwable e) {

        }
        if (null == suc) {
            error(r, t);
        } else {
            if (suc instanceof Throwable) {
                Throwable error = (Throwable) suc;
                if (null != t) {
                    String e = "future err=" + error.getMessage() + ", throwable err=" + t.getMessage();
                    log.error(e, error);
                } else {
                    log.error("get future error=" + error.getMessage(), error);
                }
            } else {
                error(r, t);
            }
        }
    }

    private final void error(Runnable r, Throwable t) {
        if (null != t) {
            log.error("runner=" + r.toString() + ", error msg=" + t.getMessage(), t);
        }
    }

    public final void terminated() {
    }

    public final void shutdown(BaojieTPool pool) {
        if (null == pool) {
            return;
        } else {
            doShutdown(pool);
        }
    }

    private final void doShutdown(BaojieTPool pool) {
        pool.shutdown();
        if (pool.isTerminated()) {
            return;
        } else {
            for (int i = 0; i < 100; i++) {
                pool.shutdown();
                if (pool.isTerminated()) {
                    break;
                } else {
                    FishSleep.park(10, TimeUnit.MILLISECONDS);
                }
            }
            if (!pool.isTerminated()) {
                pool.shutdownNow();
            }
        }
    }

    public final void cancel(List<Future<?>> futures, BaojieTPool pool) {
        if (null == futures) {
            return;
        } else {
            doCancel(futures, pool);
        }
    }

    private final void doCancel(List<Future<?>> futures, BaojieTPool pool) {
        for (Future<?> fuc : futures) {
            if (null != fuc) {
                fuc.cancel(true);
                if (null != pool) {
                    pool.purge();
                }
            }
        }
    }

}
