package com.devveri.hive.tool;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HiveHelper;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This tool generates ACL commands for all database locations
 * It allows you to use REGEX patterns to match for given databases
 */
public class ACLTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Invalid usage, try:\nACLTool <hive-host:port> <regex-filter|*> <permission>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String filter = args[1];
        final String permission = args[2];
        HiveConfig hiveConfig = new HiveConfig().setUrl(String.format("jdbc:hive2://%s", hostAndPort));
        HiveHelper hive = new HiveHelper(hiveConfig);

        System.out.println("Analyzing cluster...");

        // get database names
        Set<String> databases = hive.getDatabases();

        // apply filter
        Pattern pattern = Pattern.compile(filter.equals("*")  ? "" : filter);
        Set<String> filteredResults = filter.equals("*") ? databases :
                databases.stream().filter(pattern.asPredicate()).collect(Collectors.toSet());

        filteredResults.forEach(database -> System.out.println(String.format("hdfs dfs -setfacl --set -R %s /user/hive/warehouse/%s.db", permission, database)));
    }

}
