package com.devveri.hive.util.test.unit;

import com.devveri.hive.util.HiveUtil;
import org.junit.Test;

import static org.junit.Assert.*;

public class HiveUtilTest {

    @Test
    public void testTextFormat() {
        final String createScript =
                "CREATE TABLE `test`()\n" +
                "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' \n" +
                "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION 'hdfs://nameserver1/user/test'";
        assertEquals("Text", HiveUtil.getFormat(createScript));
    }

    @Test
    public void testParquetFormat() {
        final String createScript =
                "CREATE TABLE `test`()\n" +
                        "PARTITIONED BY (record_date STRING) \n" +
                        "COMMENT 'Main Product Table' \n" +
                        "WITH SERDEPROPERTIES ('serialization.format'='1') \n" +
                        "STORED AS PARQUET \n" +
                        "LOCATION 'hdfs://nameserver1/user/test'";
        assertEquals("Parquet", HiveUtil.getFormat(createScript));
    }

}
