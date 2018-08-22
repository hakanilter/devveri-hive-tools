package com.devveri.hive.helper;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.PartitionMetadata;
import com.devveri.hive.util.HdfsUtil;
import com.google.common.collect.Sets;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ImpalaHelper extends HiveHelper {

    private static final Set<String> RESERVED_COLUMN_NAMES = Sets.newHashSet(
            "#Rows", "#Files", "Size", "Bytes Cached", "Cache Replication", "Format", "Incremental stats", "Location");

    public ImpalaHelper(HiveConfig config) throws ClassNotFoundException {
        super(config);
    }

    public List<PartitionMetadata> getPartitionMetadata(String database, String table) throws SQLException {
        long start = System.currentTimeMillis();
        List<PartitionMetadata> list = new ArrayList<>();

        try (Connection con = getConnection(config.getUrl())) {
            // refresh table first
            try (Statement stmt = con.createStatement()) {
                stmt.execute(String.format("REFRESH %s.%s", database, table));
            }
            // get metadata
            try (PreparedStatement stmt = con.prepareStatement(String.format("SHOW PARTITIONS %s.%s", database, table));
                 ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    PartitionMetadata partitionMetadata = new PartitionMetadata();
                    // fill columns
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        if (!RESERVED_COLUMN_NAMES.contains(metaData.getColumnName(i))) {
                            Object value = rs.getObject(i);
                            partitionMetadata.getPartitionColumns().put(metaData.getColumnName(i),
                                    value instanceof String && "NULL".equalsIgnoreCase((String) value) ? null : value);
                        }
                    }
                    partitionMetadata.setRows(rs.getInt("#Rows"));
                    partitionMetadata.setFiles(rs.getInt("#Files"));
                    partitionMetadata.setSize(HdfsUtil.getFileSizeFromHumanReadable(rs.getString("Size")));
                    partitionMetadata.setFormat(rs.getString("Format"));
                    partitionMetadata.setIncrementalStats(Boolean.parseBoolean(rs.getString("Incremental stats")));
                    partitionMetadata.setLocation(rs.getString("Location"));
                    list.add(partitionMetadata);
                }
            }
        }

        long end = System.currentTimeMillis();
        logger.info("Query \"{}\" execution took {} ms", String.format("REFRESH %s.%s; SHOW PARTITIONS %s.%s",
                database, table, database, table), (end-start));

        // last record is summary record, ignore it
        return list.size() > 0 ? list.subList(0, list.size() - 1) : list;
    }

}
