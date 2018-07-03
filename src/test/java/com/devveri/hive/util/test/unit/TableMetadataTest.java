package com.devveri.hive.util.test.unit;

import com.devveri.hive.model.TableMetadata;

import org.junit.Test;

import static org.junit.Assert.*;

public class TableMetadataTest {

    @Test
    public void testExternalTable() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE EXTERNAL TABLE test (field STRING)");
        assertTrue(tableMetadata.isTable());
        assertTrue(tableMetadata.isExternal());
    }

    @Test
    public void testNonExternalTable() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE TABLE test (field STRING)");
        assertTrue(tableMetadata.isTable());
        assertFalse(tableMetadata.isExternal());
    }

    @Test
    public void testViewNonTable() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE VIEW test AS SELECT * FROM test");
        assertTrue(tableMetadata.isView());
        assertFalse(tableMetadata.isTable());
    }

    @Test
    public void testNonViewTable() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE TABLE test (field STRING)");
        assertFalse(tableMetadata.isView());
        assertTrue(tableMetadata.isTable());
    }

    @Test
    public void testPartitioned() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE TABLE test (field STRING) PARTITIONED BY (id STRING)");
        assertTrue(tableMetadata.isPartitioned());
    }

    @Test
    public void testNonPartitioned() {
        TableMetadata tableMetadata = new TableMetadata("test");
        tableMetadata.setCreateScript("CREATE TABLE test (field STRING)");
        assertFalse(tableMetadata.isPartitioned());
    }

}
