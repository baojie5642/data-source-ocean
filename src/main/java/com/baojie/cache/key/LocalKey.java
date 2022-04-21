package com.baojie.cache.key;

import com.baojie.cache.info.SourceDetail;

public class LocalKey<T extends SourceDetail> extends BaojieKey<T> {

    public LocalKey(String key, T info) {
        super(key, info);
    }

}
