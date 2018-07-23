package com.devveri.hive.tool;

import com.devveri.hive.analyzer.ClusterAnalyzer;
import com.devveri.hive.analyzer.DatabaseAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.model.ClusterMetadata;
import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.util.DDLUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ClusterReportTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Invalid usage, try:\nClusterAnalyzerTool <hive-host:port> <cluster-name>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String clusterName = args[1];
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);

        System.out.println("Analyzing cluster: " + clusterName);

        // get cluster metadata
        ClusterAnalyzer clusterAnalyzer = new ClusterAnalyzer(hiveConfig);
        ClusterMetadata clusterMetadata = clusterAnalyzer.getMetadata(clusterName);

        System.out.println("Found " + clusterMetadata.getDatabases().size() + " databases");

        System.out.println("Updating statistics...");
        clusterAnalyzer.updateStatistics(clusterMetadata);

        final String fileName = "reports.json";
        StringBuffer buffer = new StringBuffer();
        for (DatabaseMetadata databaseMetadata : clusterMetadata.getDatabases().values()) {
            if (buffer.length() > 0) {
                buffer.append(",");
            }
            buffer.append(databaseMetadata.toString());
        }
        final String reports = "var reports = [" + buffer.toString() + "];";
        Files.write(Paths.get(fileName), reports.getBytes());
        System.out.println("Report is saved as " + fileName);
    }

}
