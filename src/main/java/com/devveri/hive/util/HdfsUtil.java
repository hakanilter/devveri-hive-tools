package com.devveri.hive.util;

import com.google.common.collect.ImmutableMap;

import java.text.DecimalFormat;
import java.util.Map;

public final class HdfsUtil {

    private static final Map<String, Long> sizeMap = ImmutableMap.<String, Long>builder()
            .put("PB", (long) Math.pow(1024, 5))
            .put("TB", (long) Math.pow(1024, 4))
            .put("GB", (long) Math.pow(1024, 3))
            .put("MB", (long) Math.pow(1024, 2))
            .put("KB", (long) Math.pow(1024, 1))
            .build();

    public static long getFileSizeFromHumanReadable(String value) {
        for (Map.Entry<String, Long> entry : sizeMap.entrySet()) {
            if (value.contains(entry.getKey())) {
                return (long) (Double.parseDouble(value.replaceAll(entry.getKey(), "")) * entry.getValue());
            }
        }
        return Long.parseLong(value.replaceAll("B", ""));
    }

    public static String getHumanReadableFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + "" + units[digitGroups];
    }

    public static String getRelativePath(String path) {
        if (path.startsWith("hdfs://")) {
            if (path.indexOf(":", 5) != -1) {
                return path.substring(path.indexOf("/", path.indexOf(":", 5)));
            }
            return path.substring(path.indexOf("/", 7));
        }
        return path;
    }

    public static String getNameNodeUrl(String path) {
        if (path.startsWith("hdfs://")) {
            if (path.indexOf(":", 5) != -1) {
                return path.substring(0, path.indexOf("/", path.indexOf(":", 5)));
            }
            return path.substring(0, path.indexOf("/", 7));
        }
        return "";
    }

}
