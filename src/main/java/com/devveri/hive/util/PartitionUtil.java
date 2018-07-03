package com.devveri.hive.util;

import org.apache.commons.lang.math.NumberUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class PartitionUtil {

    public static Map<String, String> parsePartitions(String partitionLocation) {
        Map<String, String> partitionMap = new LinkedHashMap<>();

        String[] partitions = partitionLocation.split("/");
        for (String partition : partitions) {
            String[] values = partition.split("=");
            if (values.length == 2) {
                partitionMap.put(values[0], values[1]);
            }
        }

        return partitionMap;
    }

    public static String getCreateQuery(String table, String partitionLocation) {
        Map<String, String> partitionMap = parsePartitions(partitionLocation);

        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, String> entry : partitionMap.entrySet()) {
            if (buffer.length() > 0) {
                buffer.append(", ");
            }
            buffer.append(entry.getKey());
            buffer.append("=");
            if (NumberUtils.isNumber(entry.getValue())) {
                buffer.append(entry.getValue());
            } else {
                buffer.append("'");
                buffer.append(entry.getValue());
                buffer.append("'");
            }
        }

        return String.format("ALTER TABLE %s ADD PARTITION (%s) LOCATION '%s';",
                table, buffer.toString(), partitionLocation);
    }

    public static List<String> getCreateQueries(String table, Collection<String> locations) {
        return locations.stream()
                .map(location -> getCreateQuery(table, location))
                .collect(Collectors.toList());
    }

    public static String getDropQuery(String table, String partitionLocation) {
        Map<String, String> partitionMap = parsePartitions(partitionLocation);

        StringBuilder buffer = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionMap.entrySet()) {
            if (buffer.length() > 0) {
                buffer.append(", ");
            }
            buffer.append(entry.getKey());
            buffer.append("=");
            buffer.append(entry.getValue());
        }
        return String.format("ALTER TABLE %s DROP IF EXISTS PARTITION (%s);", table, buffer.toString());
    }

    public static List<String> getDropQueries(String table, Collection<String> locations) {
        return locations.stream()
                .map(location -> getDropQuery(table, location))
                .collect(Collectors.toList());
    }

}
