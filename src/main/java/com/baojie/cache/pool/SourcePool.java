package com.baojie.cache.pool;

import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.key.BaojieKey;
import com.baojie.cache.key.LocalKey;
import com.baojie.cache.util.DBHelper;
import com.baojie.cache.util.StampNum;
import com.baojie.cache.value.BaojieValue;
import com.baojie.cache.value.LocalValue;
import com.zaxxer.hikari.HikariDataSource;

public class SourcePool<K extends BaojieKey, V extends BaojieValue> extends BaojieSource<K, V> {

    private final StampNum stampNum = StampNum.getInstance();
    private final DBHelper helper = DBHelper.getInstance();

    public SourcePool(int contains, String name) {
        super(contains, name);
    }

    @Override
    public V buildSource(K key) {
        SourceDetail detail = key.info();
        HikariDataSource source = helper.hikariSource(detail);
        LocalValue<K, HikariDataSource> value = new LocalValue<>(key, source);
        return (V) value;
    }

    @Override
    public final V acquire(SourceDetail detail) {
        // String sid=stampNum.keyNum();
        String osid = detail.oceanSandID();
        BaojieKey<SourceDetail> key = new LocalKey(osid, detail);
        return acquire((K) key);
    }

}
