package com.baojie.cache.info;

public enum DBType {

    mysql(1, "mysql", "com.mysql.cj.jdbc.Driver"),
    oracle(2, "oracle", "oracle.jdbc.driver.OracleDriver");

    private final int code;
    private final String name;
    private final String driver;

    DBType(int code, String name, String driver) {
        this.code = code;
        this.name = name;
        this.driver = driver;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getDriver() {
        return driver;
    }

    @Override
    public String toString() {
        return "DBType{" +
                "code=" + code +
                ", name='" + name + '\'' +
                ", driver='" + driver + '\'' +
                '}';
    }

}
