package com.devveri.hive.tool.analyzer;

import com.devveri.hive.analyzer.DatabaseAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.util.DDLUtil;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * This tool analyzes the given database and generates DDL scripts
 */
public class DatabaseAnalyzerTool {

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Invalid usage, try:\nDatabaseAnalyzerTool <hive-host:port> <database> <include-partitions:true>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String database = args[1];
        final boolean includePartitions = args.length != 3 || Boolean.parseBoolean(args[2]);
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);

        // get database metadata
        DatabaseAnalyzer databaseAnalyzer = new DatabaseAnalyzer(hiveConfig);
        DatabaseMetadata databaseMetadata = databaseAnalyzer.getMetadata(database);

        System.out.println("Found " + databaseMetadata.getTables().size() + " tables and " + databaseMetadata.getViews().size() + " views");

        // generate ddl scripts
        String script = DDLUtil.generate(databaseMetadata, includePartitions);
        final String fileName = database + ".sql";
        Files.write(Paths.get(fileName), script.getBytes());
        System.out.println("DDL script is saved as " + fileName);
    }

}
