package com.devveri.hive.analyzer;

import com.devveri.hive.analyzer.base.BaseAnalyzer;
import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 * Analyzes a Hive database and returns table and view metadata
 */
public class DatabaseAnalyzer extends BaseAnalyzer {

    private TableAnalyzer tableAnalyzer;

    public DatabaseAnalyzer(HiveConfig config) throws Exception {
        super(config);
        tableAnalyzer = new TableAnalyzer(hive, hdfs);
    }

    public DatabaseAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        super(hive, hdfs);
        tableAnalyzer = new TableAnalyzer(hive, hdfs);
    }

    public DatabaseMetadata getMetadata(String database) throws Exception {
        DatabaseMetadata databaseMetadata = new DatabaseMetadata(database);

        // get table metadata
        hive.getTables(database).forEach(table -> {
            logger.info("Started analyzing table: " + table);
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

    public DatabaseMetadata updateTableStats(DatabaseMetadata databaseMetadata) {
        databaseMetadata.getTables().values().forEach(tableMetadata -> {
            try {
                tableAnalyzer.updateStats(tableMetadata);
                tableAnalyzer.updatePartitionStats(tableMetadata);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return databaseMetadata;
    }

}
