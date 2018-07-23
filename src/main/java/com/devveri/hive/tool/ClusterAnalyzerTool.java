package com.devveri.hive.tool;

import com.devveri.hive.analyzer.ClusterAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.ClusterMetadata;
import com.devveri.hive.util.DDLUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * This tool analyzes the whole cluster and generates DDL scripts for all databases, tables, etc.
 */
public class ClusterAnalyzerTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Invalid usage, try:\nClusterAnalyzerTool <hive-host:port> <cluster-name>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String clusterName = args[1];
        HiveConfig hiveConfig = new HiveConfig().setUrl(String.format("jdbc:hive2://%s", hostAndPort));

        System.out.println("Analyzing cluster: " + clusterName);

        // get cluster metadata
        ClusterAnalyzer clusterAnalyzer = new ClusterAnalyzer(hiveConfig);
        ClusterMetadata clusterMetadata = clusterAnalyzer.getMetadata(clusterName);

        System.out.println("Found " + clusterMetadata.getDatabases().size() + " databases");
        System.out.println("Creating DDL scripts for the cluster...");

        // generate ddl scripts
        new File(clusterName).mkdirs();
        clusterMetadata.getDatabases().values().stream().forEach(databaseMetadata -> {
            String script = DDLUtil.generate(databaseMetadata);
            final String fileName = clusterName + File.separator + databaseMetadata.getName() + ".sql";
            try {
                Files.write(Paths.get(fileName), script.getBytes());
                System.out.println("DDL script is saved in " + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}