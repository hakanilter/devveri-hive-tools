package com.devveri.hive.util.test.integration;

import com.devveri.hive.analyzer.ClusterAnalyzer;
import com.devveri.hive.model.ClusterMetadata;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ClusterAnalyzerIntegrationTest extends BaseIntegrationTest {

    private ClusterAnalyzer clusterAnalyzer;

    @Before
    public void before() throws ClassNotFoundException {
        clusterAnalyzer = new ClusterAnalyzer(
                getHiveHelperForIntegrationTest(),
                getHdfsHelperForIntegrationTest());
    }

    @Test
    public void test() throws Exception {
        ClusterMetadata clusterMetadata = clusterAnalyzer.getMetadata("test");
        assertNotNull(clusterMetadata);
        assertNotNull(clusterMetadata.getDatabases());
        System.out.println(clusterMetadata);
    }

}
