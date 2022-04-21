package com.baojie.cache.key;

import com.baojie.cache.info.SourceDetail;

public interface Key<T extends SourceDetail> {

    String getKey();

    T info();

}
