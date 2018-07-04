package com.devveri.hive.model;

import com.google.gson.Gson;

import java.util.List;
import java.util.Set;

public class TableMetadata {

    private String database;
    private String name;
    private String createScript;

    private String tableLocation;
    private List<String> folders;
    private List<String> partitions;

    private Set<String> phantomFolders;
    private Set<String> phantomPartitions;

    private long diskUsage;
    private long rowCount;

    public TableMetadata(String database, String name) {
        this.database = database;
        this.name = name;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public boolean isExternal() {
        return createScript != null && createScript.startsWith("CREATE EXTERNAL TABLE");
    }

    public boolean isPartitioned() {
        return createScript != null && createScript.contains("PARTITIONED BY");
    }

    public boolean isTable() {
        return createScript != null && (createScript.startsWith("CREATE TABLE") || createScript.startsWith("CREATE EXTERNAL TABLE"));
    }

    public boolean isView() {
        return createScript != null && createScript.startsWith("CREATE VIEW");
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

    public Set<String> getPhantomFolders() {
        return phantomFolders;
    }

    public TableMetadata setPhantomFolders(Set<String> phantomFolders) {
        this.phantomFolders = phantomFolders;
        return this;
    }

    public Set<String> getPhantomPartitions() {
        return phantomPartitions;
    }

    public TableMetadata setPhantomPartitions(Set<String> phantomPartitions) {
        this.phantomPartitions = phantomPartitions;
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

}
