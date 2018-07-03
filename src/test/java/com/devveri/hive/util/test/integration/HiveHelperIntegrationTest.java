package com.devveri.hive.util.test.integration;

import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Set;

import static org.junit.Assert.*;

public class HiveHelperIntegrationTest extends BaseIntegrationTest {

    private HiveHelper hive;

    @Before
    public void before() throws ClassNotFoundException {
        hive = getHiveHelperForIntegrationTest();
    }

    @Test
    public void testDatabases() throws SQLException {
        Set<String> databases = hive.getDatabases();
        assertNotNull(databases);
        assertTrue(databases.size() > 0);
    }

    @Test
    public void getTables() throws SQLException {
        Set<String> tables = hive.getTables("test");
        assertNotNull(tables);
        assertTrue(tables.size() > 0);
    }

    @Test
    public void getCreateTableScript() throws SQLException {
        String createScript = hive.getCreateTableScript("test", "test_table");
        assertNotNull(createScript);
    }

}
