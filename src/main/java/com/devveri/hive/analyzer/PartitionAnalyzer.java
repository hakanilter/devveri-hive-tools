package com.devveri.hive.analyzer;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.ImpalaHelper;
import com.devveri.hive.model.PartitionMetadata;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Analyzes partition information and helps optimizing tables
 */
public class PartitionAnalyzer {

    private ImpalaHelper impalaHelper;

    private Map<String, List<PartitionMetadata>> metadataCache = new HashMap<>();

    public PartitionAnalyzer(HiveConfig config) throws Exception {
        impalaHelper = new ImpalaHelper(config);
    }

    public PartitionAnalyzer(ImpalaHelper impalaHelper) {
        this.impalaHelper = impalaHelper;
    }

    /**
     * Returns number of partitions
     * Note: More than 10K partitions would be a problem
     */
    public int getNumberOfPartitions(String database, String table) throws SQLException {
        return getPartitionMetadata(database, table).size();
    }

    /**
     * Returns total number of files
     */
    public int getNumberOfFiles(String database, String table) throws SQLException {
        return getPartitionMetadata(database, table).stream().mapToInt(x -> x.getFiles()).sum();
    }

    /**
     * Returns partition columns
     */
    public Collection<String> getPartitionColumns(String database, String table) throws SQLException {
        List<PartitionMetadata> partitions = getPartitionMetadata(database, table);
        return partitions.size() == 0 ? Collections.EMPTY_LIST : partitions.get(0).getPartitionColumns().keySet();
    }

    /**
     * Returns partitions with no stats
     */
    public List<PartitionMetadata> getPartitionsWithNoStats(String database, String table) throws SQLException {
        return getPartitionMetadata(database, table).stream()
                .filter(p -> p.getFiles().equals(-1) && p.getIncrementalStats().equals(false))
                .collect(Collectors.toList());
    }

    /**
     * Returns fragmented partitions
     */
    public List<PartitionMetadata> getFragmentedPartitions(String database, String table, int maxAllowedFileCountPerPartition, long avgFileSize) throws SQLException {
        return getPartitionMetadata(database, table).stream()
                .filter(p -> ((p.getSize() / p.getFiles()) < avgFileSize) && (p.getFiles() > maxAllowedFileCountPerPartition))
                .collect(Collectors.toList());
    }

    public List<PartitionMetadata> getFragmentedPartitionsByFileCount(String database, String table, int maxAllowedFileCountPerPartition) throws SQLException {
        return getPartitionMetadata(database, table).stream()
                .filter(p -> p.getFiles() > maxAllowedFileCountPerPartition)
                .collect(Collectors.toList());
    }

    public List<PartitionMetadata> getFragmentedPartitionsBySize(String database, String table, long avgFileSize) throws SQLException {
        return getPartitionMetadata(database, table).stream()
                .filter(p -> (p.getSize() / p.getFiles()) < avgFileSize)
                .collect(Collectors.toList());
    }

    public List<PartitionMetadata> getDefaultPartitions(String database, String table) throws SQLException {
        return getPartitionMetadata(database, table).stream()
                .filter(p -> p.getPartitionColumns().values().stream()
                        .anyMatch(Objects::isNull))
                .collect(Collectors.toList());
    }

    private List<PartitionMetadata> getPartitionMetadata(String database, String table) throws SQLException {
        final String key = database + "." + table;
        if (metadataCache.containsKey(key)) {
            return metadataCache.get(key);
        }
        List<PartitionMetadata> metadata = impalaHelper.getPartitionMetadata(database, table);
        metadataCache.put(key, metadata);
        return metadata;
    }

}
