package com.devveri.hive.model;

import com.devveri.hive.util.HiveUtil;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetadata {

    private String database;
    private String name;
    private String createScript;

    private String tableLocation;
    private List<String> folders;
    private List<String> partitions;

    private Set<String> ghostFolders;
    private Set<String> ghostPartitions;

    private Map<String, Long> partitionSizeMap;

    private long diskUsage;
    private long rowCount;
    private String format;
    private boolean external;
    private boolean partitioned;
    private boolean view;

    public TableMetadata(String database, String name) {
        this.database = database;
        this.name = name;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    // getter setter

    public String getDatabase() {
        return database;
    }

    public TableMetadata setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getName() {
        return name;
    }

    public TableMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public String getCreateScript() {
        return createScript;
    }

    public TableMetadata setCreateScript(String createScript) {
        this.createScript = createScript;
        // update metadata
        format = HiveUtil.getFormat(createScript);
        external = createScript != null && createScript.startsWith("CREATE EXTERNAL TABLE");
        partitioned = createScript != null && createScript.contains("PARTITIONED BY");
        view = createScript != null && createScript.startsWith("CREATE VIEW");
        return this;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public TableMetadata setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
        return this;
    }

    public List<String> getFolders() {
        return folders;
    }

    public TableMetadata setFolders(List<String> folders) {
        this.folders = folders;
        return this;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public TableMetadata setPartitions(List<String> partitions) {
        this.partitions = partitions;
        return this;
    }

    public Set<String> getGhostFolders() {
        return ghostFolders;
    }

    public TableMetadata setGhostFolders(Set<String> ghostFolders) {
        this.ghostFolders = ghostFolders;
        return this;
    }

    public Set<String> getGhostPartitions() {
        return ghostPartitions;
    }

    public TableMetadata setGhostPartitions(Set<String> ghostPartitions) {
        this.ghostPartitions = ghostPartitions;
        return this;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public TableMetadata setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
        return this;
    }

    public long getRowCount() {
        return rowCount;
    }

    public TableMetadata setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public Map<String, Long> getPartitionSizeMap() {
        return partitionSizeMap;
    }

    public TableMetadata setPartitionSizeMap(Map<String, Long> partitionSizeMap) {
        this.partitionSizeMap = partitionSizeMap;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public Boolean isExternal() {
        return external;
    }

    public Boolean isPartitioned() {
        return partitioned;
    }

    public boolean isView() {
        return view;
    }

    public boolean isTable() {
        return !view;
    }

}
