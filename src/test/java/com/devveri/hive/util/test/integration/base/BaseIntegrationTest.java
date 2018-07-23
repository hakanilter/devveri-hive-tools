package com.devveri.hive.util.test.integration.base;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import com.devveri.hive.helper.ImpalaHelper;

public abstract class BaseIntegrationTest {

    private static final String URL = "jdbc:hive2://localhost:10000";

    protected HiveHelper getHiveHelperForIntegrationTest() throws ClassNotFoundException {
        return new HiveHelper(new HiveConfig().setUrl(URL));
    }

    protected HDFSHelper getHdfsHelperForIntegrationTest() {
        return new HDFSHelper();
    }

    protected ImpalaHelper getImpalaHelperForIntegrationTest() throws ClassNotFoundException {
        return new ImpalaHelper(new HiveConfig().setUrl(URL));
    }

}
