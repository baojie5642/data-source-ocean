package com.baojie.cache.value;

import com.baojie.cache.info.DBType;
import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.key.BaojieKey;

import javax.sql.DataSource;

public abstract class BaojieValue<K extends BaojieKey, V extends DataSource> implements Value<K, V> {

    protected final K key;
    protected final V source;
    // 默认数据源不会成功放入缓存
    protected boolean cached = false;

    protected BaojieValue(K key, V source) {
        if (null == source || null == key) {
            throw new NullPointerException("source can not be null");
        } else {
            this.source = source;
            this.key = key;
        }
    }

    @Override
    public V getSource() {
        return source;
    }

    @Override
    public K getKey() {
        return key;
    }

    public final SourceDetail getDetail() {
        return key.info();
    }

    public final DBType getDataBaseType() {
        return getDetail().getType();
    }

    public final void setInCache() {
        this.cached = true;
    }

    public final void setNotInCache() {
        this.cached = false;
    }

    public final boolean isCached() {
        boolean temp = cached;
        return temp;
    }

    public final boolean acquire() {
        return key.acquire();
    }

    // 如果是已经放入缓存
    // 那么通过key来操作
    // 如果没有放入缓存则直接关闭数据源
    public final void release() {
        try {
            key.release();
        } finally {
            if (!isCached()) {
                close();
            }
        }
    }

    public abstract void close();

}
