package com.devveri.hive.util.test.integration;

import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class HdfsHelperIntegrationTest extends BaseIntegrationTest {

    private HDFSHelper hdfs;

    @Before
    public void before() {
        hdfs = getHdfsHelperForIntegrationTest();
    }

    @Test
    public void testDirectorySize() throws IOException, URISyntaxException {
        long size = hdfs.getDirectorySize("hdfs://localhost:8020/user/test");
        assertTrue(size > 0);
    }

}
