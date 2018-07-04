package com.devveri.hive.tool;

import com.devveri.hive.analyzer.DatabaseAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.DatabaseMetadata;

import java.nio.file.Files;
import java.nio.file.Paths;

public class DatabaseReportTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Invalid usage, try:\nDatabaseReportTool <hive-host:port> <database>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String database = args[1];
        HiveConfig hiveConfig = new HiveConfig().setUrl(String.format("jdbc:hive2://%s", hostAndPort));

        // get database metadata
        DatabaseAnalyzer databaseAnalyzer = new DatabaseAnalyzer(hiveConfig);
        DatabaseMetadata databaseMetadata = databaseAnalyzer.getMetadata(database);

        System.out.println("Found " + databaseMetadata.getTables().size() + " tables and " + databaseMetadata.getViews().size() + " views");

        System.out.println("Updating statistics...");
        databaseAnalyzer.updateTableStats(databaseMetadata);

        final String fileName = "report.json";
        Files.write(Paths.get(fileName), databaseMetadata.toString().getBytes());
        System.out.println("DDL script is saved in " + fileName);
    }

}
