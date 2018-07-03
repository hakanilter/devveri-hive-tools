package com.devveri.hive.helper;

import com.devveri.hive.config.HiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class HiveHelper {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final HiveConfig config;

    public HiveHelper(HiveConfig config) throws ClassNotFoundException {
        this.config = config;
        Class.forName(config.getDriver());
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

    public String getTableLocation(String createTableScript) throws SQLException {
        // extract location, TODO use regex instead
        String startText = "LOCATION";
        if (createTableScript.indexOf(startText) == -1) {
            throw new SQLException("Table doesn't have any location metadata: " + createTableScript);
        }
        int start = createTableScript.indexOf("'", createTableScript.indexOf(startText)) + 1;
        int end = createTableScript.indexOf("'", start);
        return createTableScript.substring(start, end);
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

    private Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url, config.getUser(), config.getPassword());
    }

}
