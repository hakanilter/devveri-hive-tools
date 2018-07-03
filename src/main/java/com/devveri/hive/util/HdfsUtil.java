package com.devveri.hive.util;

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

}
