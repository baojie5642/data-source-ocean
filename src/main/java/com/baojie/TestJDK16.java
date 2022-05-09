package com.baojie;

import com.baojie.cache.info.DBType;
import com.baojie.cache.info.SourceDetail;
import com.baojie.cache.key.Key;
import com.baojie.cache.key.LocalKey;
import com.baojie.cache.pool.Pool;
import com.baojie.cache.pool.SourcePool;
import com.baojie.cache.util.DBHelper;
import com.baojie.cache.util.FishSleep;
import com.baojie.cache.value.LocalValue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

public class TestJDK16 {

    public static void main(String args[]) throws Throwable {

        SourcePool<LocalKey, LocalValue> pool = new SourcePool<>(8192, "test-pool");
        DBHelper helper = DBHelper.getInstance();
        final SourceDetail detail = new SourceDetail();
        detail.setDomain("liuxin176");
        detail.setIp("127.0.0.1");
        detail.setPort("3306");
        detail.setUser("root");
        detail.setPwd("liuxin176");
        detail.setType(DBType.mysql);
        String url = helper.mysqlJDBC("127.0.0.1", "3306", "mysql_for_test");
        detail.setJdbc(url);
        detail.setDbName("mysql_for_test");

        LocalKey test = new LocalKey(detail.oceanSandID(), detail);
        for (int i = 0; i < 999; i++) {
            LocalValue source = null;
            try {
                source = pool.getCached(test);
                if (null == source) {
                    source = pool.acquire(detail);
                }
                Key key = source.getKey();
                SourceDetail dt = key.info();
                DataSource ds = source.getSource();
                Connection connection_0 = ds.getConnection();


                connection_0.close();
                source.release();
                FishSleep.park(12, TimeUnit.SECONDS);
            } finally {
                if (null != source) {
                    source.release();
                }
            }
        }

        FishSleep.park(6, TimeUnit.MINUTES);
        pool.close();

    }

}
