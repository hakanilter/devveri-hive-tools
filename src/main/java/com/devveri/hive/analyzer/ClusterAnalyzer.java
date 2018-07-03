package com.devveri.hive.analyzer;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.ClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes a Hive cluster, returns all metadata information about all databases
 */
public class ClusterAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAnalyzer.class);

    private HiveHelper hive;
    private HDFSHelper hdfs;

    public ClusterAnalyzer(HiveConfig config) throws Exception {
        this.hive = new HiveHelper(config);
        this.hdfs = new HDFSHelper();
    }

    public ClusterAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        this.hive = hive;
        this.hdfs = hdfs;
    }

    public ClusterMetadata getMetadata(String name) throws Exception {
        ClusterMetadata clusterMetadata = new ClusterMetadata(name);

        // analyze metadata in parallel
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        // get database metadata
        DatabaseAnalyzer databaseAnalyzer = new DatabaseAnalyzer(hive, hdfs);
        hive.getDatabases().parallelStream().forEach(database -> {
            LOG.info("Started analyzing database: " + database);
            try {
                clusterMetadata.getDatabases().put(database, databaseAnalyzer.getMetadata(database));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return clusterMetadata;
    }

}
