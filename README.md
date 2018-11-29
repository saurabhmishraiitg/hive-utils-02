# hive-utils-02

Collection of general purpose UDF, UDAFs and UDTFs

# Usage

* add jar hdfs://devha/user/nexus/utils/hive-utils.jar;
* create temporary function anynull as 'org.nexus.hive.udf.GenericAnyColumnNullUDF';