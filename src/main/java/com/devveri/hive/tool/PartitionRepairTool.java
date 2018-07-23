package com.devveri.hive.tool;

import com.devveri.hive.analyzer.TableAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.TableMetadata;
import com.devveri.hive.util.PartitionUtil;

import java.util.List;
import java.util.Set;

public class PartitionRepairTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nPartitionRepairTool <hive-host:port> <database> <table>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String database = args[1];
        final String table = args[2];
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);

        TableAnalyzer analyzer = new TableAnalyzer(hiveConfig);
        TableMetadata tableMetadata = analyzer.getMetadata(database, table);

        // get phantom partitions
        Set<String> phantomFolders = tableMetadata.getPhantomFolders();
        if (phantomFolders.size() > 0) {
            System.out.println("Found " + tableMetadata.getPhantomFolders().size() + " phantom folders");
            System.out.println("Partition repair queries:\n");
            List<String> queries = PartitionUtil.getCreateQueries(table, phantomFolders);
            for (String query : queries) {
                System.out.println(query);
            }
        } else {
            System.out.println("No missing folders found");
        }
    }

}
