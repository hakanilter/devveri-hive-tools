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

}
