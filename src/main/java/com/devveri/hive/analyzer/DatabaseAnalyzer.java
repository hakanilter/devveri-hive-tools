package com.devveri.hive.analyzer;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes a Hive database and returns table and view metadata
 */
public class DatabaseAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAnalyzer.class);

    private HiveHelper hive;
    private HDFSHelper hdfs;

    public DatabaseAnalyzer(HiveConfig config) throws Exception {
        this.hive = new HiveHelper(config);
        this.hdfs = new HDFSHelper();
    }

    public DatabaseAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        this.hive = hive;
        this.hdfs = hdfs;
    }

    public DatabaseMetadata getMetadata(String database) throws Exception {
        DatabaseMetadata databaseMetadata = new DatabaseMetadata(database);

        // get table metadata
        TableAnalyzer tableAnalyzer = new TableAnalyzer(hive, hdfs);
        hive.getTables(database).stream().forEach(table -> {
            LOG.info("Started analyzing table: " + table);
            TableMetadata tableMetadata;
            try {
                tableMetadata = tableAnalyzer.getMetadata(database, table);
                if (tableMetadata.isView()) {
                    databaseMetadata.getViews().put(table, tableMetadata);
                } else {
                    databaseMetadata.getTables().put(table, tableMetadata);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return databaseMetadata;
    }

}
