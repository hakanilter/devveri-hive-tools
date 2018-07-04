package com.devveri.hive.helper;

import com.devveri.hive.config.HiveConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class HiveHelper {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    private static final Map<String, String> KNOWN_FORMATS = ImmutableMap.<String, String>builder()
            .put("STORED AS TEXTFILE", "Text")
            .put("STORED AS SEQUENCEFILE", "Sequence")
            .put("STORED AS ORC", "ORC")
            .put("STORED AS PARQUET", "Parquet")
            .put("STORED AS AVRO", "Avro")
            .put("STORED AS RCFILE", "RC")
            .put("org.apache.hadoop.mapred.TextInputFormat", "Text")
            .put("org.apache.hadoop.mapred.SequenceFileInputFormat", "Sequence")
            .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "ORC")
            .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "Parquet")
            .put("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "Avro")
            .put("org.apache.hadoop.hive.ql.io.RCFileInputFormat", "RC")
            .build();

    private static final String FORMAT_TEXT = "org.apache.hadoop.mapred.TextInputFormat";

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

    public String getFormat(String createTableScript) {
        // check known formats first
        Optional<String> tableFormat = KNOWN_FORMATS.keySet().stream()
                .filter(format -> createTableScript.contains(format)).findFirst();
        if (tableFormat.isPresent()) {
            return KNOWN_FORMATS.get(tableFormat.get());
        }
        // Unknown format
        return "-";
    }

    public String getTableLocation(String createTableScript) throws SQLException {
        // extract location, TODO use regex
        final String startText = "LOCATION";
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

    private Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url, config.getUser(), config.getPassword());
    }

}
