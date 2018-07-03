package com.devveri.hive.model;

import com.google.gson.Gson;

import java.util.List;
import java.util.Set;

public class TableMetadata {

    private String name;
    private String createScript;

    private String tableLocation;
    private List<String> folders;
    private List<String> partitions;

    private Set<String> phantomFolders;
    private Set<String> phantomPartitions;

    public TableMetadata(String name) {
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

}
