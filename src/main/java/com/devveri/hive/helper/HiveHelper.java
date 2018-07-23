package com.devveri.hive.helper;

import com.devveri.hive.config.HiveConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class HiveHelper {

    protected final Logger logger = LoggerFactory.getLogger(getClass().getName());

    protected final HiveConfig config;

    public HiveHelper(HiveConfig config) throws ClassNotFoundException {
        this.config = config;
        Class.forName(config.getDriver());

        // authentication support
        final String authentication = System.getProperty("hadoop.security.authentication");
        if (authentication != null) {
            try {
                // set the configuration
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", authentication);
                UserGroupInformation.setConfiguration(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Set<String> getDatabases() throws SQLException {
        long start = System.currentTimeMillis();

        Set<String> databases = new TreeSet<>();
        try (Connection con = getConnection(config.getUrl());
             PreparedStatement stmt = con.prepareStatement("SHOW DATABASES");
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query SHOW DATABASES execution took {} ms", (end-start));
        return databases;
    }

    public Set<String> getTables(String database) throws SQLException {
        long start = System.currentTimeMillis();

        Set<String> tables = new TreeSet<>();
        try (Connection con = getConnection(config.getUrl())) {
            try (PreparedStatement stmt = con.prepareStatement(String.format("USE %s", database))) {
                stmt.execute();
            }
            try (PreparedStatement stmt = con.prepareStatement("SHOW TABLES");
                 ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query SHOW TABLES execution took {} ms", (end-start));
        return tables;
    }

    public String getCreateTableScript(String database, String table) throws SQLException {
        long start = System.currentTimeMillis();

        final String query = String.format("SHOW CREATE TABLE %s.%s", database, table);
        StringBuffer buffer = new StringBuffer();
        try (Connection con = getConnection(config.getUrl());
            PreparedStatement stmt = con.prepareStatement(query);
            ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                if (buffer.length() > 0) {
                    buffer.append("\n");
                }
                buffer.append(rs.getString(1));
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query \"{}\" execution took {} ms", query, (end-start));
        return buffer.toString();
    }

    public List<String> getPartitionLocations(String database, String table) throws ClassNotFoundException, SQLException {
        long start = System.currentTimeMillis();
        List<String> list = new ArrayList<>();

        final String query = String.format("SHOW PARTITIONS %s.%s", database, table);
        try (Connection con = getConnection(config.getUrl());
                PreparedStatement stmt = con.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String location = rs.getString(rs.getMetaData().getColumnName(1));
                if (location != null && !"".equals(location)) {
                    list.add(location);
                }
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query \"{}\" execution took {} ms", query, (end-start));
        return list;
    }

    public long getRowCount(String database, String table) throws SQLException {
        long start = System.currentTimeMillis();

        long rowCount = 0;
        final String query = String.format("SELECT COUNT(*) FROM %s.%s", database, table);
        try (Connection con = getConnection(config.getUrl());
             PreparedStatement stmt = con.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                rowCount = rs.getLong(1);
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query \"{}\" execution took {} ms", query, (end-start));
        return rowCount;
    }

    protected Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url, config.getUser(), config.getPassword());
    }

}
