package com.devveri.hive.tool.partition;

import com.devveri.hive.analyzer.TableAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.TableMetadata;

/**
 * Reads table metadata and displays phantom partitions and folders
 */
public class PartitionAnalyzerTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nPartitionAnalyzerTool <hive-host:port> <database> <table>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String database = args[1];
        final String table = args[2];
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);

        TableAnalyzer analyzer = new TableAnalyzer(hiveConfig);
        TableMetadata tableMetadata = analyzer.getMetadata(database, table);

        // display phantom partitions
        System.out.println();
        if (tableMetadata.getPhantomPartitions().size() > 0) {
            System.out.println("Found " + tableMetadata.getPhantomPartitions().size() + " phantom partitions:");
            for (String partition : tableMetadata.getPhantomPartitions()) {
                System.out.println(partition);
            }
            System.out.println();
        } else {
            System.out.println("No phantom partitions found\n");
        }

        // display phantom folders
        if (tableMetadata.getPhantomFolders().size() > 0) {
            System.out.println("Found " + tableMetadata.getPhantomFolders().size() + " phantom folders:");
            for (String folder : tableMetadata.getPhantomFolders()) {
                System.out.println(folder);
            }
            System.out.println();
        } else {
            System.out.println("No phantom folders found");
        }
    }

}
