Devveri Hive Tools
=======

The missing tools for analyzing tables and partitions in Hive

- **Database Analyzer Tool:** 

This tool is for generating DDL scripts for a given database.

- **Cluster Analyzer Tool:** 

Same like Database Analyzer but this one generates DDL scripts for all databases, tables, etc.

- **Partition Analyzer Tool:** 

This tool reads table metadata and shows any missing partition or folder.

- **Partition Repair Tool:**

This tool checks HDFS and finds missing partitions and provides the alter queries. 

- **Phantom Partitions Tool:** 

This tool finds phantom (non existing) partitions and generates drop partition queries for them.