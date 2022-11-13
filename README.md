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
 -c org.apache.hudi.streamer.HoodieFlinkStreamerWithApollo \
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
flink run -d -t yarn-per-job -ys 2 -ynm hudi-test -c org.apache.hudi.streamer.HoodieFlinkStreamerWithApollo ./hudi-flink1.13-bundle-0.12.0.jar \
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


### Apollo Config JSON
#### COW
```
{
    "db": "mscrpt",
    "table": "invest_info_cow",
    "fields": [
        {
            "name": "id_key",
            "type": "string",
            "desc": "主键"
        },
        {
            "name": "created_at",
            "type": "timestamp",
            "desc": "创建时间"
        },
        {
            "name": "enc_user_name",
            "type": "string",
            "desc": "加密用户名"
        },
        {
            "name": "enc_user_pwd",
            "type": "string",
            "desc": "加密用户密码"
        },
        {
            "name": "invest_amount",
            "type": "decimal(10,2)",
            "desc": "投资金额"
        },
        {
            "name": "updated_at",
            "type": "timestamp",
            "desc": "更新时间"
        },
        {
            "name": "user_level",
            "type": "int",
            "desc": "用户等级"
        },
        {
            "name": "user_sex",
            "type": "int",
            "desc": "用户性别，0 男 1 女"
        },
        {
            "name": "ts",
            "type": "string",
            "desc": "分区字段"
        }
    ],
    "kafka_config": {
        "topic": "invest_info",
        "group_id": "t04",
        "bootstrap_server": "hadoop001:9092"
    },
    "hive_sync_config": {
        "metastore_uris": "thrift://hadoop001:9083",
        "sync_db": "mscrpt",
        "sync_table": "invest_info_cow",
        "hive_partition_field": "ts"
    },
    "hudi_config": {
        "table_type": "COPY_ON_WRITE",
        "base_path": "hdfs:///hudi/mscrpt/invest_info_cow",
        "hudi_table_name": "invest_info_cow",
        "record_key_field": "id_key",
        "precombine_field": "updated_at",
        "hudi_partition_field": "created_at"
    }
}
```

#### MOR
```
{
    "db": "mscrpt",
    "table": "invest_info_mor",
    "fields": [
        {
            "name": "id_key",
            "type": "string",
            "desc": "主键"
        },
        {
            "name": "created_at",
            "type": "timestamp",
            "desc": "创建时间"
        },
        {
            "name": "enc_user_name",
            "type": "string",
            "desc": "加密用户名"
        },
        {
            "name": "enc_user_pwd",
            "type": "string",
            "desc": "加密用户密码"
        },
        {
            "name": "invest_amount",
            "type": "decimal(10,2)",
            "desc": "投资金额"
        },
        {
            "name": "updated_at",
            "type": "timestamp",
            "desc": "更新时间"
        },
        {
            "name": "user_level",
            "type": "int",
            "desc": "用户等级"
        },
        {
            "name": "user_sex",
            "type": "int",
            "desc": "用户性别，0 男 1 女"
        },
        {
            "name": "ts",
            "type": "string",
            "desc": "分区字段"
        }
    ],
    "kafka_config": {
        "topic": "invest_info",
        "group_id": "t01",
        "bootstrap_server": "hadoop001:9092"
    },
    "hive_sync_config": {
        "metastore_uris": "thrift://hadoop001:9083",
        "sync_db": "mscrpt",
        "sync_table": "invest_info_mor",
        "hive_partition_field": "ts"
    },
    "hudi_config": {
        "table_type": "MERGE_ON_READ",
        "base_path": "hdfs:///hudi/mscrpt/invest_info_mor",
        "hudi_table_name": "invest_info_mor",
        "record_key_field": "id_key",
        "precombine_field": "updated_at",
        "hudi_partition_field": "created_at"
    }
}
```


### HoodieMuiltiTableFlinkStreamerWithApollo
```
flink run -d -t yarn-per-job \
-ynm hudi-test \
-c org.apache.hudi.streamer.HoodieMuiltiTableFlinkStreamerWithApollo \
./hudi-flink1.13-bundle-0.12.0.jar \
--checkpoint-interval 10000 \
--apollo-config-key mscrpt.invest_info.config
```