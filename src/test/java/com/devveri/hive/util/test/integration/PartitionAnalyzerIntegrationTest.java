package com.devveri.hive.util.test.integration;

import com.devveri.hive.analyzer.PartitionAnalyzer;
import com.devveri.hive.model.PartitionMetadata;
import com.devveri.hive.util.test.integration.base.BaseIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class PartitionAnalyzerIntegrationTest extends BaseIntegrationTest {

    private PartitionAnalyzer partitionAnalyzer;

    @Before
    public void before() throws ClassNotFoundException {
        partitionAnalyzer = new PartitionAnalyzer(getImpalaHelperForIntegrationTest());
    }

    @Test
    public void testPartitionsWithNoStats() throws SQLException {
        List<PartitionMetadata> partitionsWithNoStats = partitionAnalyzer.getPartitionsWithNoStats("default", "test_table");
        assertNotNull(partitionsWithNoStats);
        assertTrue(partitionsWithNoStats.size() > 0);
    }

    @Test
    public void testFragmentedPartitions() throws SQLException {
        final int maxAllowedFileCountPerPartition = 1;
        List<PartitionMetadata> fragmentedPartitions = partitionAnalyzer.getFragmentedPartitionsByFileCount("default", "test_table", maxAllowedFileCountPerPartition);
        assertNotNull(fragmentedPartitions);
        assertTrue(fragmentedPartitions.size() > 0);
    }

}
