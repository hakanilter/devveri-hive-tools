package com.devveri.hive.util;

import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.model.TableMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class DDLUtil {

    public static String generate(DatabaseMetadata databaseMetadata, boolean includePartitions) {
        StringBuffer buffer = new StringBuffer();

        buffer.append("-- Autogenerated DDL script for database ");
        buffer.append(databaseMetadata.getName());
        buffer.append("\n-- Generated at ");
        buffer.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        buffer.append("\n");

        buffer.append("-- Found ");
        buffer.append(databaseMetadata.getTables().size());
        buffer.append(" tables and ");
        buffer.append(databaseMetadata.getViews().size());
        buffer.append(" views\n");

        // create database
        buffer.append(String.format("CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n",
                databaseMetadata.getName(), databaseMetadata.getName()));

        // add table definitions
        if (databaseMetadata.getTables().size() > 0) {
            buffer.append("-- Table Definitions\n");
            for (TableMetadata tableMetadata : databaseMetadata.getTables().values()) {
                // table definition
                buffer.append("-- ");
                buffer.append(tableMetadata.getName());
                buffer.append("\n");
                buffer.append(tableMetadata.getCreateScript());
                buffer.append(";\n");
                if (tableMetadata.isPartitioned() && includePartitions) {
                    // partitions
                    buffer.append("-- Partitions\n");
                    for (String partitionLocation : tableMetadata.getPartitions()) {
                        buffer.append(PartitionUtil.getCreateQuery(tableMetadata.getName(), partitionLocation));
                        buffer.append("\n");
                    }
                    buffer.append("\n");
                }
                buffer.append("\n");
            }
            buffer.append("\n");
        }

        // add view definitions
        if (databaseMetadata.getViews().size() > 0) {
            buffer.append("-- View Definitions\n");
            for (TableMetadata viewMetadata : databaseMetadata.getViews().values()) {
                buffer.append("-- ");
                buffer.append(viewMetadata.getName());
                buffer.append("\n");
                buffer.append(viewMetadata.getCreateScript());
                buffer.append(";\n\n");
            }
            buffer.append("\n\n");
        }

        return buffer.toString();
    }


}
