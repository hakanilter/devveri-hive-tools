package com.devveri.hive.util.test.unit;

import com.devveri.hive.util.HdfsUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HdfsUtilTest {

    @Test
    public void testRelativePath() {
        assertEquals("/user/hive/warehouse/database.db/table", HdfsUtil.getRelativePath("hdfs://somehost01.testnamenode:80/user/hive/warehouse/database.db/table"));
        assertEquals("/user/hive/warehouse/database.db/table", HdfsUtil.getRelativePath("hdfs://somehost01.testnamenode:8020/user/hive/warehouse/database.db/table"));
        assertEquals("/user/hive/warehouse/database.db/table", HdfsUtil.getRelativePath("hdfs://nameserver1/user/hive/warehouse/database.db/table"));
        assertEquals("/user/hive/warehouse/database.db/table", HdfsUtil.getRelativePath("/user/hive/warehouse/database.db/table"));
    }

    @Test
    public void testNameNodeUrl() {
        assertEquals("hdfs://somehost01.testnamenode:80", HdfsUtil.getNameNodeUrl("hdfs://somehost01.testnamenode:80/user/hive/warehouse/database.db/table"));
        assertEquals("hdfs://somehost01.testnamenode:8020", HdfsUtil.getNameNodeUrl("hdfs://somehost01.testnamenode:8020/user/hive/warehouse/database.db/table"));
        assertEquals("hdfs://nameserver1", HdfsUtil.getNameNodeUrl("hdfs://nameserver1/user/hive/warehouse/database.db/table"));
        assertEquals("", HdfsUtil.getNameNodeUrl("/user/hive/warehouse/database.db/table"));
    }

    @Test
    public void testFileSizeFromHumanReadable() {
        assertEquals(900, HdfsUtil.getFileSizeFromHumanReadable("900B"));
        assertEquals(2L * (long) Math.pow(1024, 1), HdfsUtil.getFileSizeFromHumanReadable("2KB"));
        assertEquals(3L * (long) Math.pow(1024, 2), HdfsUtil.getFileSizeFromHumanReadable("3MB"));
        assertEquals(4L * (long) Math.pow(1024, 3), HdfsUtil.getFileSizeFromHumanReadable("4GB"));
        assertEquals(5L * (long) Math.pow(1024, 4), HdfsUtil.getFileSizeFromHumanReadable("5TB"));
    }

    @Test
    public void testHumanReadableFileSize() {
        assertEquals("900B", HdfsUtil.getHumanReadableFileSize(900));
        assertEquals("2KB", HdfsUtil.getHumanReadableFileSize((long) Math.pow(1024, 1) * 2L));
        assertEquals("3MB", HdfsUtil.getHumanReadableFileSize((long) Math.pow(1024, 2) * 3L));
        assertEquals("4GB", HdfsUtil.getHumanReadableFileSize((long) Math.pow(1024, 3) * 4L));
        assertEquals("5TB", HdfsUtil.getHumanReadableFileSize((long) Math.pow(1024, 4) * 5L));
    }

}
