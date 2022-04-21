package com.baojie.cache.value;

public interface Value<K, V> extends AutoCloseable {

    K getKey();

    V getSource();

}
