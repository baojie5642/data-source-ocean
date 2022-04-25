package com.baojie.cache.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.baojie.cache.info.DBType;
import com.baojie.cache.info.SourceDetail;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.Properties;

public class DBHelper {

    private static volatile DBHelper instance;

    private DBHelper() {

    }

    public static final DBHelper getInstance() {
        DBHelper copy = instance;
        if (null != copy) {
            return copy;
        } else {
            synchronized (DBHelper.class) {
                copy = instance;
                if (null == copy) {
                    copy = instance = new DBHelper();
                }
            }
        }
        return copy;
    }


    public final DruidDataSource druidSource(SourceDetail detail) {
        String proxy_key = detail.proxyKey();
        // 判断是否能使用useInformationSchema来获取数据源
        // 将不能使用的过滤掉
        boolean special = testProxy(proxy_key);
        // 通过代理链接的数据源
        if (special) {
            detail.setProxy(true);
        }
        final DruidDataSource source = new DruidDataSource();
        // source.setExceptionSorter("");
        source.setUseUnfairLock(true);
        // 基本属性 url、user、password
        source.setUrl(detail.getJdbc());
        source.setUsername(detail.getUser());
        source.setPassword(detail.getPwd());
        // 初始化大小、最小、最大
        source.setInitialSize(4);
        source.setMinIdle(2);
        source.setMaxActive(128);
        // 获取连接等待超时的时间
        source.setMaxWait(180_000L);
        // 间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        // config.getTimeBetweenEvictionRunsMillis()
        source.setTimeBetweenEvictionRunsMillis(60_000L);
        // 连接出错时候,需要重连的间隔时间
        source.setTimeBetweenConnectErrorMillis(60_000L);
        // 设置获取连接出错时的自动重连次数
        source.setConnectionErrorRetryAttempts(1);
        // 设置获取连接时的重试次数,-1为不重试,
        // 默认为1,此处设置为2
        // 由于内部是for循环获取防止阻塞外部调用线程
        // 不能设置成为retry的次数
        source.setNotFullTimeoutRetryCount(1);
        // 重试连接失败后跳出循环
        // true表示向数据库请求连接失败后,
        // 就算后端数据库恢复正常也不进行重连,
        // 客户端对pool的请求都拒绝掉.
        // false表示新的请求都会尝试去数据库请求connection.默认为false
        source.setBreakAfterAcquireFailure(true);
        source.setTestOnBorrow(true);
        // 申请连接的时候检测
        // 如果空闲时间大于timeBetweenEvictionRunsMillis
        // 执行validationQuery检测连接是否有效
        source.setTestWhileIdle(true);
        source.setKillWhenSocketReadTimeout(true);
        source.setRemoveAbandoned(true);
        // 三分钟
        source.setMinEvictableIdleTimeMillis(1000L * 60L * 3L);
        // 五分钟
        source.setMaxEvictableIdleTimeMillis(1000L * 60L * 5L);
        // 对于不能使用useInformationSchema来获取数据源的也可以设置此属性
        source.addConnectionProperty("remarks", "true");
        if (DBType.oracle == detail.getType()) {
            source.setDriverClassName(DBType.oracle.getDriver());
            source.setDbType(DBType.oracle.getName());
            // 获取Oracle元数据 REMARKS信息
            if (!special) {
                source.addConnectionProperty("remarksReporting", "true");
            }
        } else {
            // 获取MySQL元数据 REMARKS信息
            if (!special) {
                source.addConnectionProperty("useInformationSchema", "true");
            }
            source.setDriverClassName(DBType.mysql.getDriver());
            source.setDbType(DBType.mysql.getName());
        }
        return source;
    }

    private final boolean testProxy(String proxyKey) {
        return false;
    }

    public final HikariDataSource hikariSource(SourceDetail detail) {
        String proxy_key = detail.proxyKey();
        // 判断是否能使用useInformationSchema来获取数据源
        // 将不能使用的过滤掉
        boolean special = testProxy(proxy_key);
        // 通过代理链接的数据源
        if (special) {
            detail.setProxy(true);
        }
        final HikariConfig hikariConfig = hikariConfig(detail);
        Properties properties = new Properties();
        properties.put("remarks", "true");
        if (DBType.oracle == detail.getType()) {
            // 获取Oracle元数据 REMARKS信息
            if (!special) {
                properties.put("remarksReporting", "true");
            }
            // 在测试链接的时候需要区分 oracle 与 mysql 数据库使用的测试语句
            hikariConfig.setConnectionTestQuery("select 1 from dual");
        } else if (DBType.mysql == detail.getType()) {
            // 获取MySQL元数据 REMARKS信息
            if (!special) {
                properties.put("useInformationSchema", "true");
            }
            // 在测试链接的时候需要区分 oracle 与 mysql 数据库使用的测试语句
            hikariConfig.setConnectionTestQuery("select 1");
        }
        hikariConfig.setDataSourceProperties(properties);
        final HikariDataSource source = new HikariDataSource(hikariConfig);
        return source;
    }

    private final HikariConfig hikariConfig(SourceDetail detail) {
        final HikariConfig config = new HikariConfig();
        String osid = detail.oceanSandID();
        config.setPoolName(osid);
        String url = detail.getJdbc();
        config.setJdbcUrl(url);
        String un = detail.getUser();
        config.setUsername(un);
        String pwd = detail.getPwd();
        config.setPassword(pwd);
        String dn = detail.getType().getDriver();
        config.setDriverClassName(dn);
        // 池中最大连接数,包括闲置和使用中的连接
        // 扩大链接到 128
        config.setMaximumPoolSize(128);
        // 池中维护的最小空闲连接数
        config.setMinimumIdle(8);
        // 从数据库链接池获取connection的等待时间
        config.setConnectionTimeout(180_000L);
        // 连接允许在池中闲置的最长时间
        config.setIdleTimeout(300_000L);
        // 池中连接最长生命周期
        config.setMaxLifetime(1800_000L);
        // 请参考源代码注释
        // 如果设置为负值虽然应用启动没问题但是链接不能重连
        // 暂时设置 6 秒
        // config.setInitializationFailTimeout(6_000L);
        // 为了测试不进行初始链接探测
        config.setInitializationFailTimeout(-1L);
        return config;
    }

    public final String mysqlJDBC(String ip, String port, String schema) {
        final StringBuilder builder = new StringBuilder(256);
        builder.append("jdbc:mysql://");
        builder.append(ip);
        builder.append(":");
        builder.append(port);
        builder.append("/");
        builder.append(schema);
        builder.append("?useUnicode=true");
        builder.append("&characterEncoding=utf8");
        builder.append("&zeroDateTimeBehavior=convertToNull");
        builder.append("&serverTimezone=Asia/Shanghai");
        builder.append("&nullCatalogMeansCurrent=true");
        // 添加一个重连,和版本有关
        // https://blog.csdn.net/hzbooks/article/details/108215836
        // builder.append("&autoReconnect=true");
        builder.append("&useSSL=false");
        return builder.toString();
    }

    public final String oracleJDBC(String ip, String port, String serviceName) {
        final StringBuilder builder = new StringBuilder(256);
        builder.append("jdbc:oracle:thin:@");
        builder.append(ip);
        builder.append(":");
        builder.append(port);
        builder.append("/");
        builder.append(serviceName);
        return builder.toString();
    }

}
