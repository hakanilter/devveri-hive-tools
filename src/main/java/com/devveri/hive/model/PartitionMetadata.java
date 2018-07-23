package com.devveri.hive.model;

import com.google.gson.Gson;

import java.util.Map;
import java.util.TreeMap;

public class PartitionMetadata {

    private Map<String, Object> partitionColumns = new TreeMap<>();

    private Integer rows;

    private Integer files;

    private Long size;

    private String format;

    private Boolean incrementalStats;

    private String location;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    // getter setter

    public Map<String, Object> getPartitionColumns() {
        return partitionColumns;
    }

    public PartitionMetadata setPartitionColumns(Map<String, Object> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    public Integer getRows() {
        return rows;
    }

    public PartitionMetadata setRows(Integer rows) {
        this.rows = rows;
        return this;
    }

    public Integer getFiles() {
        return files;
    }

    public PartitionMetadata setFiles(Integer files) {
        this.files = files;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public PartitionMetadata setSize(Long size) {
        this.size = size;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public PartitionMetadata setFormat(String format) {
        this.format = format;
        return this;
    }

    public Boolean getIncrementalStats() {
        return incrementalStats;
    }

    public PartitionMetadata setIncrementalStats(Boolean incrementalStats) {
        this.incrementalStats = incrementalStats;
        return this;
    }

    public String getLocation() {
        return location;
    }

    public PartitionMetadata setLocation(String location) {
        this.location = location;
        return this;
    }

}
