package com.devveri.hive.model;

import com.google.gson.Gson;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseMetadata {

    private String name;

    private Map<String, TableMetadata> tables = new ConcurrentHashMap<>();
    private Map<String, TableMetadata> views = new ConcurrentHashMap<>();

    public DatabaseMetadata(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    // getter setter

    public String getName() {
        return name;
    }

    public DatabaseMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public Map<String, TableMetadata> getTables() {
        return tables;
    }

    public DatabaseMetadata setTables(Map<String, TableMetadata> tables) {
        this.tables = tables;
        return this;
    }

    public Map<String, TableMetadata> getViews() {
        return views;
    }

    public DatabaseMetadata setViews(Map<String, TableMetadata> views) {
        this.views = views;
        return this;
    }

}
