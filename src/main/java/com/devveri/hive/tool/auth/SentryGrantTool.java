package com.devveri.hive.tool.auth;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HiveHelper;

import java.nio.file.Files;
import java.nio.file.Paths;
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
        HiveConfig hiveConfig = new HiveConfig().setUrl(hostAndPort);
        HiveHelper hive = new HiveHelper(hiveConfig);

        System.out.println("Analyzing cluster...");

        // get database names
        Set<String> databases = hive.getDatabases();

        // apply filter
        Pattern pattern = Pattern.compile(filter.equals("*")  ? "" : filter);
        Set<String> filteredResults = filter.equals("*") ? databases :
                databases.stream().filter(pattern.asPredicate()).collect(Collectors.toSet());

        // generate file
        StringBuffer buffer = new StringBuffer();
        filteredResults.forEach(database -> buffer.append(String.format("GRANT %s ON DATABASE %s TO ROLE %s;\n", action, database, roleName)));

        final String fileName = String.format("sentry-%s.sql", System.currentTimeMillis());
        Files.write(Paths.get(fileName), buffer.toString().getBytes());
        System.out.println("Sentry script is saved as " + fileName);
    }

}
