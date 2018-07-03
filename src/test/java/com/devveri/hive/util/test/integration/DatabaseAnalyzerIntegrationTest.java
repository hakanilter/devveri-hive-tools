package com.devveri.hive.util.test.integration;

import com.devveri.hive.analyzer.DatabaseAnalyzer;
import com.devveri.hive.model.DatabaseMetadata;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DatabaseAnalyzerIntegrationTest extends BaseIntegrationTest {

    private DatabaseAnalyzer databaseAnalyzer;

    @Before
    public void before() throws ClassNotFoundException {
        databaseAnalyzer = new DatabaseAnalyzer(
                getHiveHelperForIntegrationTest(),
                getHdfsHelperForIntegrationTest());
    }

    @Test
    public void test() throws Exception {
        DatabaseMetadata databaseMetadata = databaseAnalyzer.getMetadata("test");
        assertNotNull(databaseMetadata.getTables());
        assertNotNull(databaseMetadata.getViews());
        // TODO add more checks
        System.out.println(databaseMetadata);
    }

}
