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

import static com.devveri.hive.config.HiveConstants.*;

/**
 * This tool uses Impala to retrieve partition metadata and analyzes it to identify possible problems.
 * It also generates required queries to fix these issues.
 */
public class TableHealthTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nPartitionRepairTool <hive-host:port> <database> <table>");
            System.exit(-1);
        }
        new TableHealthTool().run(args[0], args[1], args[2]);
    }

    protected PartitionAnalyzer partitionAnalyzer;

    private void run(final String hostAndPort, final String database, final String table) throws Exception {
        this.partitionAnalyzer = new PartitionAnalyzer(new HiveConfig().setUrl(hostAndPort));

        System.out.printf("Analyzing table %s.%s\n", database, table);

        // check table health
        int numberOfPartitions = checkNumberOfPartitions(database, table);
        checkPartitionDepth(database, table);
        List<String> defragQueries = checkFragmentation(database, table, numberOfPartitions);
        List<String> statQueries = checkStats(database, table, numberOfPartitions);
        checkDefaultValues(database, table);

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
            final String fileName = String.format("table-fix-%s.sql", System.currentTimeMillis());
            Files.write(Paths.get(fileName), buffer.toString().getBytes());
            System.out.println("Table fix queries are saved as " + fileName);
        }
    }

    protected int checkNumberOfPartitions(String database, String table) throws Exception {
        int numberOfPartitions = partitionAnalyzer.getNumberOfPartitions(database, table);
        boolean tooManyPartitions = numberOfPartitions > MAX_PARTITION_COUNT;
        if (tooManyPartitions) {
            System.err.printf("Checking partition count... [FAILED], Found %d partitions. More than %d is considered as unhealthy. You should consider changing your partition strategy.\n", numberOfPartitions, MAX_PARTITION_COUNT);
        } else {
            System.out.println("Checking partition count... [PASSED]");
        }
        return numberOfPartitions;
    }

    protected void checkPartitionDepth(String database, String table) throws Exception  {
        Collection<String> partitionColumns = partitionAnalyzer.getPartitionColumns(database, table);
        boolean deepPartitioning = partitionColumns.size() > MAX_PARTITION_DEPTH;
        if (deepPartitioning) {
            String partitions = partitionColumns.stream().collect(Collectors.joining("/"));
            System.err.printf("Checking partition depth... [FAILED], Found %d levels of partitioning (%s). You should consider changing your partition strategy to minimize number of small files and increase query performance.\n", partitionColumns.size(), partitions);
        } else {
            System.out.println("Checking partition depth... [PASSED]");
        }
    }

    protected List<String> checkStats(String database, String table, int numberOfPartitions) throws Exception {
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

    protected List<String> checkFragmentation(String database, String table, int numberOfPartitions) throws Exception {
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

    protected void checkDefaultValues(String database, String table) throws Exception {
        List<PartitionMetadata> partitionsWithDefaultValue = partitionAnalyzer.getDefaultPartitions(database, table);
        boolean hasDefaultValue = partitionsWithDefaultValue.size() > 0;
        if (hasDefaultValue) {
            System.err.printf("Checking null values... [FAILED], Found %d partitions are using null values.\n", partitionsWithDefaultValue.size());
        } else {
            System.out.println("Checking null values... [PASSED]");
        }
    }

}
