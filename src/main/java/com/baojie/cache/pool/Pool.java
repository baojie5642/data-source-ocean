package com.baojie.cache.pool;

import com.baojie.cache.info.SourceDetail;

public interface Pool<K, V> extends AutoCloseable {

    // 仅仅对 key 中的值进行探测
    V getCached(K k);

    // 需要 key 中有详细的值
    V acquire(K k);

    // 需要 detail 中有详细的值
    V acquire(SourceDetail detail);

    boolean release(V v);

    void close();

}
