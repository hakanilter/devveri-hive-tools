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

    public void run(final String hostAndPort, final String database, final boolean includePartitions) throws Exception {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Invalid usage, try:\nDatabaseAnalyzerTool <hive-host:port> <database> <include-partitions:true>");
            System.exit(-1);
        }
        new DatabaseAnalyzerTool().run(args[0], args[1], args.length != 3 || Boolean.parseBoolean(args[2]));
    }

}
