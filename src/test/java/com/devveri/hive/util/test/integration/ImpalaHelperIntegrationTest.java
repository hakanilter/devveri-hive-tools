package com.devveri.hive.util.test.integration;

import com.devveri.hive.helper.ImpalaHelper;
import com.devveri.hive.model.PartitionMetadata;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class ImpalaHelperIntegrationTest extends BaseIntegrationTest {

    @Test
    public void testPartitionMetadata() throws ClassNotFoundException, SQLException {
        ImpalaHelper impala = getImpalaHelperForIntegrationTest();
        List<PartitionMetadata> partitions = impala.getPartitionMetadata("default", "test_table");
        assertNotNull(partitions);
        assertTrue(partitions.size() > 0);
    }

}
