package com.devveri.hive.tool.health;

import com.devveri.hive.analyzer.PartitionAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HiveHelper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import static com.devveri.hive.config.HiveConstants.HIVE_OPTIONS;
import static com.devveri.hive.config.HiveConstants.IMPALA_OPTIONS;

public class DatabaseHealthTool extends TableHealthTool {

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Invalid usage, try:\nPartitionRepairTool <hive-host:port> <database> <file-name=optional>");
            System.exit(-1);
        }
        new DatabaseHealthTool().run(args[0], args[1], args.length == 3 ? args[2] : null);
    }

    private final boolean useHive = System.getProperty("useHive") == null ? false : Boolean.parseBoolean(System.getProperty("useHive"));

    private void run(final String hostAndPort, final String database, final String optionalFileName) throws Exception {
        HiveHelper hive = new HiveHelper(new HiveConfig().setUrl(hostAndPort));
        this.partitionAnalyzer = new PartitionAnalyzer(new HiveConfig().setUrl(hostAndPort));

        System.out.printf("Analyzing database %s\n", database);

        StringBuffer buffer = new StringBuffer();
        for (String table : hive.getTables(database)) {
            System.out.printf("\nAnalyzing table %s.%s\n", database, table);
            try {
                // check table health
                int numberOfPartitions = checkNumberOfPartitions(database, table);
                checkPartitionDepth(database, table);
                List<String> defragQueries = checkFragmentation(database, table, numberOfPartitions);
                List<String> statQueries = checkStats(database, table, numberOfPartitions);
                checkDefaultValues(database, table);

                // update script
                if (defragQueries.size() + statQueries.size() > 0) {
                    buffer.append("--- Table: ").append(table);
                    if (defragQueries.size() > 0) {
                        if (buffer.length() > 0) {
                            buffer.append("\n");
                        }
                        buffer.append("-- These queries will merge small files\n");
                        defragQueries.forEach(q -> buffer.append(q).append("\n"));
                    }
                    if (statQueries.size() > 0) {
                        if (buffer.length() > 0) {
                            buffer.append("\n");
                        }
                        buffer.append("-- These queries will update stats\n");
                        statQueries.forEach(q -> buffer.append(q).append("\n"));
                    }
                }
            } catch (SQLException e) {
                System.err.printf("No partition information found for the table: %s, skipping...\n", e.getSQLState());
            }
        }

        if (buffer.length() > 0) {
            final String fileName = optionalFileName == null ? String.format("database-fix-%s.sql", System.currentTimeMillis()) : optionalFileName;
            Files.write(Paths.get(fileName), ((useHive ? HIVE_OPTIONS : IMPALA_OPTIONS) + buffer.toString()).getBytes());
            System.out.println("Database fix queries are saved as " + fileName);
        }
    }

}
