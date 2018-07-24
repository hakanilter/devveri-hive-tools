package com.devveri.hive.tool;

import com.devveri.hive.analyzer.PartitionAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.PartitionMetadata;
import com.devveri.hive.util.PartitionUtil;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This tool uses Impala to retrieve partition metadata and analyzes it to identify possible problems.
 * It also generates required queries to fix these issues.
 */
public class TableHealthTool {

    private static final int MAX_PARTITION_COUNT = 10000;

    private static final int MAX_PARTITION_DEPTH = 3;

    private static final int MAX_ALLOWED_FILES_PER_PARTITION = 1;

    private static final int COMPUTE_STATS_THRESHOLD = 50;

    public static void main(String[] args) throws Exception {
        new TableHealthTool().run(args);
    }

    private String database, table;

    private PartitionAnalyzer partitionAnalyzer;

    public void run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nPartitionRepairTool <hive-host:port> <database> <table>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        this.database = args[1];
        this.table = args[2];
        this.partitionAnalyzer = new PartitionAnalyzer(new HiveConfig().setUrl(hostAndPort));

        System.out.printf("Analyzing table %s.%s\n", database, table);

        // check table health
        int numberOfPartitions = checkNumberOfPartitions();
        checkPartitionDepth();
        List<String> defragQueries = checkFragmentation(numberOfPartitions);
        List<String> statQueries = checkStats(numberOfPartitions);
        checkDefaultValues();

        // generate file
        StringBuffer buffer = new StringBuffer();
        if (defragQueries.size() > 0) {
            buffer.append("-- These queries will merge small files\n");
            defragQueries.forEach(q -> {
                buffer.append(q);
                buffer.append("\n");
            });
        }
        if (statQueries.size() > 0) {
            if (buffer.length() > 0) {
                buffer.append("\n");
            }
            buffer.append("-- These queries will update stats\n");
            statQueries.forEach(q -> {
                buffer.append(q);
                buffer.append("\n");
            });
        }

        if (buffer.length() > 0) {
            final String fileName = String.format("fix-%s.sql", System.currentTimeMillis());
            Files.write(Paths.get(fileName), buffer.toString().getBytes());
            System.out.println("Table fix queries are saved as " + fileName);
        }
    }

    private int checkNumberOfPartitions() throws Exception {
        int numberOfPartitions = partitionAnalyzer.getNumberOfPartitions(database, table);
        boolean tooManyPartitions = numberOfPartitions > MAX_PARTITION_COUNT;
        if (tooManyPartitions) {
            System.err.printf("Checking partition count... [FAILED], Found %d partitions. More than %d is considered as unhealthy. You should consider changing your partition strategy.\n", numberOfPartitions, MAX_PARTITION_COUNT);
        } else {
            System.out.println("Checking partition count... [PASSED]");
        }
        return numberOfPartitions;
    }

    private void checkPartitionDepth() throws Exception  {
        Collection<String> partitionColumns = partitionAnalyzer.getPartitionColumns(database, table);
        boolean deepPartitioning = partitionColumns.size() > MAX_PARTITION_DEPTH;
        if (deepPartitioning) {
            String partitions = partitionColumns.stream().collect(Collectors.joining("/"));
            System.err.printf("Checking partition depth... [FAILED], Found %d levels of partitioning (%s). You should consider changing your partition strategy to minimize number of small files and increase query performance.\n", partitionColumns.size(), partitions);
        } else {
            System.out.println("Checking partition depth... [PASSED]");
        }
    }

    private List<String> checkStats(int numberOfPartitions) throws Exception {
        List<PartitionMetadata> partitionsWithNoStats = partitionAnalyzer.getPartitionsWithNoStats(database, table);
        boolean hasNoStats = partitionsWithNoStats.size() > 0;
        if (hasNoStats) {
            double rate = (double) partitionsWithNoStats.size() / (double) numberOfPartitions * 100;
            System.err.printf("Checking table stats... [FAILED], Found %d of %d partitions (%%%d) have no stats.\n", partitionsWithNoStats.size(), numberOfPartitions, (int) rate);
            if (rate > COMPUTE_STATS_THRESHOLD) {
                return Collections.singletonList(String.format("COMPUTE INCREMENTAL STATS %s.%s;", database, table));
            } else {
                return PartitionUtil.getComputeIncrementalStatsQueries(database, table, partitionsWithNoStats);
            }
        } else {
            System.out.println("Checking table stats... [PASSED]");
        }
        return Collections.EMPTY_LIST;
    }

    private List<String> checkFragmentation(int numberOfPartitions) throws Exception {
        List<PartitionMetadata> fragmentedPartitions = partitionAnalyzer.getFragmentedPartitions(database, table, MAX_ALLOWED_FILES_PER_PARTITION);
        boolean fragmentation = fragmentedPartitions.size() > 0;
        if (fragmentation) {
            double rate = (double) fragmentedPartitions.size() / (double) numberOfPartitions * 100;
            System.err.printf("Checking small files... [FAILED], Found %d of %d partitions (%%%d) are fragmented.\n", fragmentedPartitions.size(), numberOfPartitions, (int) rate);
            return PartitionUtil.getMergeQueries(database, table, fragmentedPartitions);
        } else {
            System.out.println("Checking small files... [PASSED]");
        }
        return Collections.EMPTY_LIST;
    }

    private void checkDefaultValues() throws Exception {
        List<PartitionMetadata> partitionsWithDefaultValue = partitionAnalyzer.getDefaultPartitions(database, table);
        boolean hasDefaultValue = partitionsWithDefaultValue.size() > 0;
        if (hasDefaultValue) {
            System.err.printf("Checking null values... [FAILED], Found %d partitions are using null values.\n", partitionsWithDefaultValue.size());
        } else {
            System.out.println("Checking null values... [PASSED]");
        }
    }

}
