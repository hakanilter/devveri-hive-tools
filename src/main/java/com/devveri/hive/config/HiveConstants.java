package com.devveri.hive.config;

public interface HiveConstants {

    int MAX_PARTITION_COUNT = 10000;

    int MAX_PARTITION_DEPTH = 3;

    int MAX_ALLOWED_FILES_PER_PARTITION = 1;

    long AVG_FILE_SIZE_PER_PARTITION = 1024L * 1024L * 128L; // 128 MB

    int COMPUTE_STATS_THRESHOLD = 50;

    String HIVE_OPTIONS = "SET hive.vectorized.execution.enabled=true;\n" +
            "SET hive.vectorized.execution.reduce.enabled=true;\n" +
            "SET hive.cbo.enable=true;\n" +
            "SET hive.compute.query.using.stats=true;\n" +
            "SET hive.stats.fetch.column.stats=true;\n" +
            "SET hive.stats.fetch.partition.stats=true;\n" +
            "SET parquet.compress=SNAPPY;\n" +
            "SET parquet.compression=SNAPPY;\n" +
            "SET hive.merge.mapfiles=true;\n" +
            "SET hive.merge.mapredfiles=true;\n" +
            "SET hive.merge.size.per.task=128000000;\n" +
            "SET hive.exec.dynamic.partition=true;\n" +
            "SET hive.exec.dynamic.partition.mode=nonstrict;\n" +
            "SET mapreduce.map.memory.mb=2048;\n" +
            "SET mapreduce.reduce.memory.mb=2048;\n" +
            "SET hive.exec.dynamic.partition.mode=nonstrict;\n\n";

    String IMPALA_OPTIONS = "SET num_nodes=1;\n\n";

}
