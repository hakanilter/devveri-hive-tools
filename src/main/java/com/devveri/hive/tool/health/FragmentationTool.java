package com.devveri.hive.tool.health;

import com.devveri.hive.analyzer.PartitionAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.PartitionMetadata;

import java.util.ArrayList;
import java.util.List;

public class FragmentationTool {

    public void run(final String hostAndPort) throws Exception {
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);
        HiveHelper hive = new HiveHelper(hiveConfig);
        PartitionAnalyzer partitionAnalyzer = new PartitionAnalyzer(hiveConfig);

        List<String> list = new ArrayList<>();
        for (String database: hive.getDatabases()) {
            for (String table : hive.getTables(database)) {
                System.out.printf("Analyzing table %s.%s\n", database, table);
                try {
                    int numberOfFiles = partitionAnalyzer.getNumberOfFiles(database, table);
                    int numberOfPartitions = partitionAnalyzer.getNumberOfPartitions(database, table);
                    List<PartitionMetadata> fragmentedPartitions = partitionAnalyzer.getFragmentedPartitionsByFileCount(database, table, 1);
                    double rate = (double) fragmentedPartitions.size() / (double) numberOfPartitions * 100;
                    final String line = String.format("%s\t%s\t%d\t%d\t%%%d", database, table, numberOfFiles, numberOfPartitions, (int) rate);
                    list.add(line);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }

        System.out.println("Database Name\tTable Name\tNumber Of Files\tNumber of Partitions\tFrag Rate");
        list.forEach(System.out::println);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Invalid usage, try:\nFragmentationTool <hive-host:port>");
            System.exit(-1);
        }
        new FragmentationTool().run(args[0]);
    }

}
