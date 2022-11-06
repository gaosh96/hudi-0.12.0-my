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

### Without Sync to hive
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