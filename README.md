# DevVeri Hive Tools

The missing tools for analyzing tables and partitions in Hive

## Getting Started

DevVeri Hive Tools provides various useful tool for managing Hive tables and partitions. It will also help you for solving the following problems: 

- Small Files Problem: Many people having this issue due to frequently updated Hive tables. 
- Manually added partitions
- Ghost Partitions 
- Generating DDL Scripts, including partitions, etc. 

## Tools

- **Database Analyzer Tool:** 

This tool is for generating DDL scripts for a given database.

```bash
$ ./database-analyze.sh default
2018-09-04 16:17:54 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-04 16:17:54 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-04 16:17:54 INFO  HiveHelper:73 - Query SHOW TABLES execution took 170 ms
2018-09-04 16:17:54 INFO  DatabaseAnalyzer:38 - Started analyzing table: nyse
2018-09-04 16:17:54 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-04 16:17:54 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-04 16:17:54 INFO  HiveHelper:94 - Query "SHOW CREATE TABLE default.nyse" execution took 15 ms
2018-09-04 16:17:54 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Failed to read directory information: hdfs://quickstart.cloudera:8020/user/hive/warehouse/nyse
2018-09-04 16:17:55 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-04 16:17:55 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-04 16:17:55 INFO  HiveHelper:115 - Query "SHOW PARTITIONS default.nyse" execution took 58 ms
2018-09-04 16:17:55 INFO  DatabaseAnalyzer:38 - Started analyzing table: nyse_external
2018-09-04 16:17:55 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-04 16:17:55 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-04 16:17:55 INFO  HiveHelper:94 - Query "SHOW CREATE TABLE default.nyse_external" execution took 14 ms
Failed to read directory information: hdfs://quickstart.cloudera:8020/user/cloudera/nyse_external
Found 2 tables and 0 views
DDL script is saved as default.sql

real    0m1.261s
user    0m1.203s
sys     0m0.422s
```

- **Cluster Analyzer Tool:** 

Same like Database Analyzer but this one generates DDL scripts for all databases, tables, etc.

- **Table Health Tool:** 

This tool reads table metadata using Impala, checks table health and generates scripts to fix the problems. The tool checks for;

- Number of partitions should be less than 10000 for performance reasons
- Partition depth should be less than 3, otherwise it causes both small files problem and performance issues. 
- Small files problem, each partition must have 1 file (you can set that value with -DmaxFilesPerPartition=$VALUE)
- Table statistics enabled or not
- Partition with NULL or default values

The generated scripts can either run Impala or Hive. For Hive queries -DuseHive=true must be added in $JAVA_OPTS.

The following example shows how to fix small files problem. In the first run the tool shows that the table has multiple files for each partition:  

```bash
$ ./table-health.sh default nyse
Analyzing table default.nyse
2018-09-05 08:00:03 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-05 08:00:03 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-05 08:00:05 INFO  ImpalaHelper:57 - Query "REFRESH default.nyse; SHOW PARTITIONS default.nyse" execution took 1860 ms
Total 1557 files found in 519 partitions
Checking partition count... [PASSED]
Checking partition depth... [PASSED]
Checking small files... [FAILED], Found 519 of 519 partitions (%100) are fragmented.
Checking table stats... [PASSED]
Checking null values... [PASSED]
Table fix queries are saved as table-fix-1536134405346.sql

real    0m2.953s
user    0m1.234s
sys     0m0.422s
```

A script file called table-fix-TIMESTAMP.sql is created, here is the content:

```mysql-sql
SET num_nodes=1;

-- These queries will merge small files
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-03';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-04';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-05';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-06';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-07';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-10';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-11';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-12';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-13';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-14';
INSERT OVERWRITE TABLE default.nyse PARTITION (stock_date) SELECT * FROM default.nyse WHERE stock_date='2000-01-18';
...
```

If you run these queries and check the table again, you will see that small files are merged:

```bash
$ ./table-health.sh default nyse
Analyzing table default.nyse
2018-09-05 09:07:28 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-05 09:07:28 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-05 09:07:29 INFO  ImpalaHelper:57 - Query "REFRESH default.nyse; SHOW PARTITIONS default.nyse" execution took 845 ms
Total 519 files found in 519 partitions
Checking partition count... [PASSED]
Checking partition depth... [PASSED]
Checking small files... [PASSED]
Checking table stats... [PASSED]
Checking null values... [PASSED]

real    0m1.685s
user    0m0.875s
sys     0m0.313s
```

- **Database Health Tool:** 

Same like Table Health tool but this checks the whole database and generate scripts.

```bash
$ ./database-health.sh default
Analyzing database default
2018-09-05 08:02:43 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-05 08:02:43 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-05 08:02:43 INFO  HiveHelper:73 - Query SHOW TABLES execution took 397 ms

Analyzing table default.nyse
2018-09-05 08:02:43 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-05 08:02:43 INFO  Utils:383 - Resolved authority: localdocker:21050
2018-09-05 08:02:44 INFO  ImpalaHelper:57 - Query "REFRESH default.nyse; SHOW PARTITIONS default.nyse" execution took 807 ms
Total 1557 files found in 519 partitions
Checking partition count... [PASSED]
Checking partition depth... [PASSED]
Checking small files... [FAILED], Found 519 of 519 partitions (%100) are fragmented.
Checking table stats... [PASSED]
Checking null values... [PASSED]

Analyzing table default.nyse_external
2018-09-05 08:02:44 INFO  Utils:295 - Supplied authorities: localdocker:21050
2018-09-05 08:02:44 INFO  Utils:383 - Resolved authority: localdocker:21050
No partition information found for the table: HY000, skipping...
Database fix queries are saved as database-fix-1536134564672.sql

real    0m1.821s
user    0m0.906s
sys     0m0.297s
```

- **Partition Analyzer Tool:** 

This tool reads table metadata and shows any missing partition or folder. 

- **Partition Repair Tool:**

This tool checks HDFS and finds missing partitions and provides the alter queries. 

- **Phantom Partitions Tool:** 

This tool finds phantom (non existing) partitions and generates drop partition queries for them.
