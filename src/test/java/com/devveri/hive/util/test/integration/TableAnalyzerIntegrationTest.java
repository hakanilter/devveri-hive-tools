package com.devveri.hive.util.test.integration;

import com.devveri.hive.analyzer.TableAnalyzer;
import com.devveri.hive.model.TableMetadata;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;

import org.junit.Before;
import org.junit.Test;

public class TableAnalyzerIntegrationTest extends BaseIntegrationTest {

    private TableAnalyzer tableAnalyzer;

    @Before
    public void before() throws ClassNotFoundException {
        tableAnalyzer = new TableAnalyzer(
                getHiveHelperForIntegrationTest(),
                getHdfsHelperForIntegrationTest());
    }

    @Test
    public void test() throws Exception {
        TableMetadata tableMetadata = tableAnalyzer.getMetadata("test", "test_table");
        System.out.println(tableMetadata);
    }

}
