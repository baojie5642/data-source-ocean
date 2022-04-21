package com.baojie.cache.pool;

import com.baojie.cache.info.SourceDetail;

public interface Pool<K, V> extends AutoCloseable {

    V acquire(K k);

    V acquire(SourceDetail detail);

    boolean release(V v);

    void close();

}
