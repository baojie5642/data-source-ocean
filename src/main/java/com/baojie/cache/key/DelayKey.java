package com.baojie.cache.key;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DelayKey<K> implements Delayed, Serializable {

    private static final long serialVersionUID = -2017031516252955555L;
    private static final AtomicLong adder = new AtomicLong();

    private final TimeUnit unit;
    private final long sequence;
    private final long takeTime;
    private final long delay;
    private final K k;

    public DelayKey(K k, long delay, TimeUnit unit) {
        innerCheck(k, delay, unit);
        this.takeTime = takeTime(delay, unit);
        this.sequence = adder.getAndIncrement();
        this.delay = delay;
        this.unit = unit;
        this.k = k;
    }

    private final void innerCheck(K k, long delay, TimeUnit unit) {
        if (null == k) {
            throw new NullPointerException();
        }
        if (delay <= 0L) {
            throw new IllegalStateException();
        }
        if (null == unit) {
            throw new NullPointerException();
        }
    }

    private final long takeTime(long delay, TimeUnit unit) {
        return TimeUnit.NANOSECONDS.convert(delay, unit) + System.nanoTime();
    }

    @Override
    public final long getDelay(final TimeUnit unit) {
        return unit.convert(takeTime - now(), NANOSECONDS);
    }

    private final long now() {
        return System.nanoTime();
    }

    @Override
    public final int compareTo(final Delayed other) {
        if (null == other) {
            return 1;
        }
        if (other == this) {
            return 0;
        }
        if (other instanceof DelayKey) {
            DelayKey x = DelayKey.class.cast(other);
            long diff = takeTime - x.takeTime;
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else if (sequence < x.sequence) {
                return -1;
            } else {
                return 1;
            }
        } else {
            final long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }
    }

    public final K getKey() {
        return k;
    }

    @Override
    public String toString() {
        return "DelayKey{" +
                "unit=" + unit +
                ", sequence=" + sequence +
                ", delay=" + delay +
                ", takeTime=" + takeTime +
                ", k=" + k +
                '}';
    }
}
