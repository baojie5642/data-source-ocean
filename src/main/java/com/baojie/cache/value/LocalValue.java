package com.baojie.cache.value;

import com.alibaba.druid.pool.DruidDataSource;
import com.baojie.cache.key.BaojieKey;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class LocalValue<K extends BaojieKey, V extends DataSource> extends BaojieValue<K, V> {

    public LocalValue(K key, V source) {
        super(key, source);
    }

    @Override
    public void close() {
        try {
            mayDruid();
        } finally {
            doClose();
        }
    }

    public final void mayDruid() {
        final DataSource copy = source;
        if (null != copy) {
            if (copy instanceof DruidDataSource) {
                DruidDataSource temp = DruidDataSource.class.cast(copy);
                try {
                    temp.setTestWhileIdle(false);
                    temp.setKeepAlive(false);
                    temp.setKillWhenSocketReadTimeout(true);
                    temp.setBreakAfterAcquireFailure(true);
                    temp.setConnectionErrorRetryAttempts(1);
                    temp.setTestOnReturn(false);
                    temp.setPhyTimeoutMillis(1);
                    temp.setTimeBetweenEvictionRunsMillis(1);
                    temp.setConnectionErrorRetryAttempts(1);
                    temp.setTimeBetweenConnectErrorMillis(1);
                    temp.setNotFullTimeoutRetryCount(0);
                    temp.setBreakAfterAcquireFailure(true);
                    // 下面的设置可能抛异常
                    // 修改成30001,防止error日志的打印
                    temp.setMinEvictableIdleTimeMillis(30001);
                    temp.setKeepAliveBetweenTimeMillis(30001);
                    temp.setMaxEvictableIdleTimeMillis(30001);
                } catch (Throwable t) {
                    // ignore
                }
            }
        }
    }

    private final void doClose() {
        final DataSource copy = source;
        if (null != copy) {
            try {
                if (copy instanceof HikariDataSource) {
                    HikariDataSource temp = HikariDataSource.class.cast(copy);
                    temp.close();
                } else if (copy instanceof DruidDataSource) {
                    DruidDataSource source = DruidDataSource.class.cast(copy);
                    source.close();
                } else {
                    String cn = copy.getClass().getName();
                    System.err.println("unknow class name = " + cn);
                }
            } catch (Throwable t) {

            }
        }
    }

}
