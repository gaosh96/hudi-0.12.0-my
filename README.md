# Apache Hudi

Multiple Kafka Topics Sink to Multiple Hudi Table in one Flink Job

## Building from source

```shell
mvn clean install \
-DskipTests \
-DskipITs \
-Drat.skip=true \
-Dcheckstyle.skip=true \
-Dhadoop.version=3.0.0-cdh6.2.1 \
-Dhive.version=2.1.1-cdh6.2.1 \
-Pflink1.13 \
-Pflink-bundle-shade-hive2 \ 
-Pscala-2.11
```

## QuickStart

### COW Without Sync to hive
```shell
flink run -d -t yarn-per-job -ynm hudi-test \
-Dclassloader.check-leaked-classloader=false \
 -c org.apache.hudi.streamer.HoodieMuiltiTableFlinkStreamer \
./hudi-flink1.13-bundle-0.12.0.jar \
--kafka-topic invest_info \
--kafka-group-id test01 \
--kafka-bootstrap-servers hadoop01:9092 \
--target-base-path hdfs:///hudi/mscrpt/invest_info \
--target-table invest_info \
--table-type COPY_ON_WRITE \
--record-key-field id_key \
--source-ordering-field updated_at \
--partition-path-field updated_at \
--apollo-config-key 'invest_info.schema' \
--hive-style-partitioning `true`
```

### COW Sync to Hive
```
flink run -d -t yarn-per-job -ys 2 -ynm hudi-test -c org.apache.hudi.streamer.HoodieMuiltiTableFlinkStreamer ./hudi-flink1.13-bundle-0.12.0.jar \
--kafka-topic invest_info \
--kafka-group-id test04 \
--kafka-bootstrap-servers hadoop001:9092 \
--target-base-path hdfs:///hudi/mscrpt/invest_info3 \
--target-table invest_info \
--table-type COPY_ON_WRITE \
--record-key-field id_key \
--source-ordering-field updated_at \
--partition-path-field created_at \
--apollo-config-key invest_info.schema \
--write-partition-format 'yyyy-MM-dd' \
--hive-sync-partition-extractor-class org.apache.hudi.hive.HiveStylePartitionValueExtractor \
--hive-style-partitioning `true` \
--hive-sync-enable `true` \
--hive-sync-db mscrpt \
--hive-sync-table invest_info3 \
--hive-sync-mode hms \
--hive-sync-metastore-uris thrift://hadoop001:9083 \
--hive-sync-partition-fields dw_updated_at \
--hive-sync-support-timestamp `true`
```