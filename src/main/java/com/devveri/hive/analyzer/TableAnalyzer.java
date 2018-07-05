package com.devveri.hive.analyzer;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.model.TableMetadata;
import com.devveri.hive.util.CollectionUtil;
import com.devveri.hive.util.HdfsUtil;
import com.devveri.hive.util.HiveUtil;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;

/**
 * Analyzes table metadata using JDBC client, returns location and partition information
 */
public class TableAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(TableAnalyzer.class);

    private HiveHelper hive;
    private HDFSHelper hdfs;

    public TableAnalyzer(HiveConfig config) throws Exception {
        this.hive = new HiveHelper(config);
        this.hdfs = new HDFSHelper();
    }

    public TableAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        this.hive = hive;
        this.hdfs = hdfs;
    }

    public TableMetadata getMetadata(String database, String table) throws Exception {
        TableMetadata tableMetadata = new TableMetadata(database, table);
        tableMetadata.setCreateScript(hive.getCreateTableScript(database, table));

        if (tableMetadata.isTable()) {
            String path = HiveUtil.getLocation(tableMetadata.getCreateScript());
            tableMetadata.setTableLocation(path);
            // remove hdfs namenode url from the create script
            String nameNodeUrl = HdfsUtil.getNameNodeUrl(path);
            tableMetadata.setCreateScript(tableMetadata.getCreateScript().replaceAll(nameNodeUrl, ""));
            try {
                tableMetadata.setFolders(hdfs.getDeepestDirectories(tableMetadata.getTableLocation()));
            } catch (AccessControlException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Don't have an access to foder: " + tableMetadata.getTableLocation(), e);
                } else {
                    System.err.println("Don't have an access to foder: " + tableMetadata.getTableLocation());
                }
                tableMetadata.setFolders(Collections.EMPTY_LIST);
            } catch (FileNotFoundException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Folder doesn't exist: " + tableMetadata.getTableLocation(), e);
                } else {
                    System.err.println("Folder doesn't exist: " + tableMetadata.getTableLocation());
                }
                tableMetadata.setFolders(Collections.EMPTY_LIST);
            }
            if (tableMetadata.isPartitioned()) {
                tableMetadata.setPartitions(hive.getPartitionLocations(database, table));
                tableMetadata.setPhantomFolders(CollectionUtil.diff(tableMetadata.getFolders(), tableMetadata.getPartitions()));
                tableMetadata.setPhantomPartitions(CollectionUtil.diff(tableMetadata.getPartitions(), tableMetadata.getFolders()));
            } else {
                tableMetadata.setPartitions(Collections.EMPTY_LIST);
            }
        }

        return tableMetadata;
    }

    public TableMetadata updateStats(TableMetadata tableMetadata) throws IOException, URISyntaxException, SQLException {
        long size = hdfs.getDirectorySize(tableMetadata.getTableLocation());
        long rowCount = hive.getRowCount(tableMetadata.getDatabase(), tableMetadata.getName());
        tableMetadata.setDiskUsage(size);
        tableMetadata.setRowCount(rowCount);
        return tableMetadata;
    }

}
