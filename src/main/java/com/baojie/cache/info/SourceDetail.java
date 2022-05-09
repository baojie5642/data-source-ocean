package com.baojie.cache.info;

public class SourceDetail {

    private String domain;              // 用户的域名账号名称,可以理解为是用户的别名或者是对应公司内部的员工id
    private String dsid;                // 数据源 id
    private String user;                // 用户名
    private String pwd;                 // 密码
    private String jdbc;                // 连接串
    private String dbName;              // 数据库名称
    private String ip;                  // ip
    private String port;                // 端口
    private DBType type = DBType.mysql; // 数据源类型
    private boolean proxy = false;      // 是否是一个代理的数据源,主要针对mysql或者oracle数据库
    private String proxyKey = "";

    public SourceDetail() {

    }

    public String getDsid() {
        return dsid;
    }

    public void setDsid(String dsid) {
        this.dsid = dsid;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
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

    // 作为数据库数据源的唯一标记
    public final String oceanSandID() {
        String domain = getDomain();
        String user = getUser();
        String dbType = getType().getName();
        String ip = getIp();
        String port = getPort();
        String dbn = getDbName();
        StringBuilder builder = new StringBuilder();
        builder.append(domain);
        builder.append("-");
        builder.append(user);
        builder.append("-");
        builder.append(dbType);
        builder.append("-");
        builder.append(ip);
        builder.append("-");
        builder.append(port);
        builder.append("-");
        builder.append(dbn);
        return builder.toString();
    }

    @Override
    public String toString() {
        return "SourceDetail{" +
                "domain='" + domain + '\'' +
                ", user='" + user + '\'' +
                ", jdbc='" + jdbc + '\'' +
                ", dbName='" + dbName + '\'' +
                ", ip='" + ip + '\'' +
                ", port='" + port + '\'' +
                ", type=" + type +
                ", dsid=" + dsid +
                ", proxy=" + proxy +
                ", proxyKey='" + proxyKey + '\'' +
                '}';
    }

}
