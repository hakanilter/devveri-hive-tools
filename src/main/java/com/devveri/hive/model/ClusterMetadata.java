package com.devveri.hive.model;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class ClusterMetadata {

    private String name;

    private Map<String, DatabaseMetadata> databases = new HashMap<>();

    public ClusterMetadata(String name) {
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

    public ClusterMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public Map<String, DatabaseMetadata> getDatabases() {
        return databases;
    }

    public ClusterMetadata setDatabases(Map<String, DatabaseMetadata> databases) {
        this.databases = databases;
        return this;
    }

}
