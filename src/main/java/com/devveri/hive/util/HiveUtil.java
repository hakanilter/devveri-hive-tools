package com.devveri.hive.util;

import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class HiveUtil {

    private static final Map<String, String> KNOWN_FORMATS = ImmutableMap.<String, String>builder()
            .put("STORED AS TEXTFILE", "Text")
            .put("STORED AS SEQUENCEFILE", "Sequence")
            .put("STORED AS ORC", "ORC")
            .put("STORED AS PARQUET", "Parquet")
            .put("STORED AS AVRO", "Avro")
            .put("STORED AS RCFILE", "RC")
            .put("org.apache.hadoop.mapred.TextInputFormat", "Text")
            .put("org.apache.hadoop.mapred.SequenceFileInputFormat", "Sequence")
            .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "ORC")
            .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "Parquet")
            .put("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "Avro")
            .put("org.apache.hadoop.hive.ql.io.RCFileInputFormat", "RC")
            .build();

    public static String getFormat(String createTableScript) {
        // check known formats first
        Optional<String> tableFormat = KNOWN_FORMATS.keySet().stream()
                .filter(format -> createTableScript.contains(format)).findFirst();
        if (tableFormat.isPresent()) {
            return KNOWN_FORMATS.get(tableFormat.get());
        }
        // Unknown format
        return "-";
    }

    public static String getLocation(String createTableScript) throws SQLException {
        // extract location, TODO use regex
        final String startText = "LOCATION";
        if (createTableScript.indexOf(startText) == -1) {
            throw new SQLException("Table doesn't have any location metadata: " + createTableScript);
        }
        int start = createTableScript.indexOf("'", createTableScript.indexOf(startText)) + 1;
        int end = createTableScript.indexOf("'", start);
        return createTableScript.substring(start, end);
    }

}

