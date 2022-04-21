package com.baojie.cache.info;

public class SourceDetail {

    private String user;                // 用户名
    private String pwd;                 // 密码
    private String jdbc;                // 连接串
    private String dbName;              // 数据库名称
    private String ip;                  // ip
    private String port;                // 端口
    private DBType type;                // 数据源类型
    private boolean proxy = false;      // 是否是一个代理的数据源,主要针对mysql或者oracle数据库
    private String proxyKey = "";

    public SourceDetail() {

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getJdbc() {
        return jdbc;
    }

    public void setJdbc(String jdbc) {
        this.jdbc = jdbc;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public DBType getType() {
        return type;
    }

    public void setType(DBType type) {
        this.type = type;
    }

    public boolean isProxy() {
        return proxy;
    }

    public void setProxy(boolean proxy) {
        this.proxy = proxy;
    }

    public final String proxyKey() {
        String dbn = getDbName();
        String ip = getIp();
        String port = getPort();
        StringBuilder builder = new StringBuilder();
        builder.append(ip);
        builder.append(":");
        builder.append(port);
        builder.append(":");
        builder.append(dbn);
        return builder.toString();
    }

    @Override
    public String toString() {
        return "SourceDetail{" +
                "user='" + user + '\'' +
                ", jdbc='" + jdbc + '\'' +
                ", dbName='" + dbName + '\'' +
                ", ip='" + ip + '\'' +
                ", port='" + port + '\'' +
                ", type=" + type +
                ", proxy=" + proxy +
                ", proxyKey='" + proxyKey + '\'' +
                '}';
    }


}
