package com.devveri.hive.tool;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HiveHelper;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This tool generates GRANT commands for all databases
 * It allows you to use REGEX patterns to match for given databases
 * Example:
 *      SentryGrantTool "locahost:10000" "[w]*_temp" "ALL" "my_role"
 */
public class SentryGrantTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Invalid usage, try:\nSentryGrantTool <hive-host:port> <regex-filter|*> <action> <role-name>");
            System.exit(-1);
        }

        final String hostAndPort = args[0];
        final String filter = args[1];
        final String action = args[2];
        final String roleName = args[3];
        HiveConfig hiveConfig = new HiveConfig().setUrl(String.format("jdbc:hive2://%s", hostAndPort));
        HiveHelper hive = new HiveHelper(hiveConfig);

        System.out.println("Analyzing cluster...");

        // get database names
        Set<String> databases = hive.getDatabases();

        // apply filter
        Pattern pattern = Pattern.compile(filter.equals("*")  ? "" : filter);
        Set<String> filteredResults = filter.equals("*") ? databases :
                databases.stream().filter(pattern.asPredicate()).collect(Collectors.toSet());

        filteredResults.forEach(database -> System.out.println(String.format("GRANT %s ON DATABASE %s TO ROLE %s;", action, database, roleName)));
    }

}
