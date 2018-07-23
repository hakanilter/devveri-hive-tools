package com.devveri.hive.analyzer.base;

import com.devveri.hive.config.HiveConfig;
import com.devveri.hive.helper.HDFSHelper;
import com.devveri.hive.helper.HiveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnalyzer {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected HiveHelper hive;
    protected HDFSHelper hdfs;

    public BaseAnalyzer(HiveConfig config) throws Exception {
        this.hive = new HiveHelper(config);
        this.hdfs = new HDFSHelper();
    }

    public BaseAnalyzer(HiveHelper hive, HDFSHelper hdfs) {
        this.hive = hive;
        this.hdfs = hdfs;
    }

}
