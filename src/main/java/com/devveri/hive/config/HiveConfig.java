package com.devveri.hive.config;

public class HiveConfig {

    private String driver = "org.apache.hive.jdbc.HiveDriver";
    private String url = "jdbc:hive2://localhost:10000/;auth=noSasl";
    private String user = "";
    private String password = "";

    public String getDriver() {
        return driver;
    }

    public HiveConfig setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public HiveConfig setUrl(String url) {
        if (url != null && !url.startsWith("jdbc:")) {
            this.url = "jdbc:hive2://" + url;
        } else {
            this.url = url;
        }
        return this;
    }

    public String getUser() {
        return user;
    }

    public HiveConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public HiveConfig setPassword(String password) {
        this.password = password;
        return this;
    }

}
