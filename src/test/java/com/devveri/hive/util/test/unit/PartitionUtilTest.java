package com.devveri.hive.util.test.unit;

import com.devveri.hive.util.PartitionUtil;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class PartitionUtilTest {

    @Test
    public void testParsePartitions() {
        final String location = "/user/hive/warehouse/database.db/tablename/a=20141112/b=1/c=3";
        Map<String, String> partitionMap = PartitionUtil.parsePartitions(location);
        assertNotNull(partitionMap);
        assertEquals(3, partitionMap.size());
        assertTrue(partitionMap.containsKey("a"));
        assertTrue(partitionMap.containsKey("b"));
        assertTrue(partitionMap.containsKey("c"));
        assertEquals("20141112", partitionMap.get("a"));
        assertEquals("1", partitionMap.get("b"));
        assertEquals("3", partitionMap.get("c"));
    }

    @Test
    public void testDropQuery() {
        final String table = "tablename", location = "hdfs://nameservice1/user/hive/warehouse/database.db/tablename/a=20141112/b=1/c=3";
        String query = PartitionUtil.getDropQuery(table, location);
        assertNotNull(query);
        assertEquals("ALTER TABLE tablename DROP IF EXISTS PARTITION (a=20141112, b=1, c=3);", query);
    }

}
