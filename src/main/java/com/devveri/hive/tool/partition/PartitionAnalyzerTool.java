package com.devveri.hive.tool.partition;

import com.devveri.hive.analyzer.TableAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.TableMetadata;
import com.devveri.hive.util.PartitionUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads table metadata and displays ghost partitions and folders
 */
public class PartitionAnalyzerTool {

    public void run(final String hostAndPort, final String database, final String table) throws Exception {
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);

        TableAnalyzer analyzer = new TableAnalyzer(hiveConfig);
        TableMetadata tableMetadata = analyzer.getMetadata(database, table);

        // display ghost partitions
        System.out.println();
        if (tableMetadata.getGhostPartitions().size() > 0) {
            System.out.println("-- Found " + tableMetadata.getGhostPartitions().size() + " ghost partition(s):");
            List<String> ghostPartitions = PartitionUtil.getDropQueries(table, tableMetadata.getGhostPartitions());
            ghostPartitions.forEach(System.out::println);
            System.out.println();
        } else {
            System.out.println("-- No ghost partitions found\n");
        }

        // display phantom folders
        if (tableMetadata.getGhostFolders().size() > 0) {
            System.out.println("-- Found " + tableMetadata.getGhostFolders().size() + " ghost folder(s):");
            List<String> ghostFolders = PartitionUtil.getCreateQueries(table, tableMetadata.getGhostFolders());
            ghostFolders.forEach(System.out::println);
            System.out.println();
        } else {
            System.out.println("-- No ghost folders found");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nPartitionAnalyzerTool <hive-host:port> <database> <table>");
            System.exit(-1);
        }
        new PartitionAnalyzerTool().run(args[0], args[1], args[2]);
    }

}
