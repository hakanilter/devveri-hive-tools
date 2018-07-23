package com.devveri.hive.analyzer;

import com.devveri.hive.analyzer.base.BaseAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.ClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes a Hive cluster, returns all metadata information about all databases
 */
public class ClusterAnalyzer extends BaseAnalyzer {

    private DatabaseAnalyzer databaseAnalyzer;

    public ClusterAnalyzer(HiveConfig config) throws Exception {
        super(config);
        databaseAnalyzer = new DatabaseAnalyzer(hive, hdfs);
    }

    public ClusterAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        super(hive, hdfs);
        databaseAnalyzer = new DatabaseAnalyzer(hive, hdfs);
    }

    public ClusterMetadata getMetadata(String name) throws Exception {
        ClusterMetadata clusterMetadata = new ClusterMetadata(name);

        // analyze metadata in parallel
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        // get database metadata
        hive.getDatabases().parallelStream().forEach(database -> {
            logger.info("Started analyzing database: " + database);
            try {
                clusterMetadata.getDatabases().put(database, databaseAnalyzer.getMetadata(database));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return clusterMetadata;
    }

    public void updateStatistics(ClusterMetadata clusterMetadata) {
        clusterMetadata.getDatabases().values().parallelStream().forEach(databaseMetadata -> {
            databaseAnalyzer.updateTableStats(databaseMetadata);
        });
    }

}
