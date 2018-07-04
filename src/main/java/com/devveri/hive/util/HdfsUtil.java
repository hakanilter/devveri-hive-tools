package com.devveri.hive.util;

import java.text.DecimalFormat;

public final class HdfsUtil {

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

    public static String getHumanReadableFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

}
