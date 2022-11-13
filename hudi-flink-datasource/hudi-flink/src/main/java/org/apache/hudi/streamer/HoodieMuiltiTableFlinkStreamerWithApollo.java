/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.streamer;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.beust.jcommander.JCommander;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hive.HiveStylePartitionValueExtractor;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.MultiTableStringToRowDataMapFunction;
import org.apache.hudi.util.SchemaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A utility which can incrementally consume data from Kafka and apply it to the target table.
 * It has the similar functionality with SQL data source except that the source is bind to Kafka
 * and the format is bind to JSON.
 */
public class HoodieMuiltiTableFlinkStreamerWithApollo {

    private static final Logger LOG = LoggerFactory.getLogger(HoodieMuiltiTableFlinkStreamerWithApollo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
        JCommander cmd = new JCommander(cfg, null, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }

        env.enableCheckpointing(cfg.checkpointInterval);
        env.getConfig().setGlobalJobParameters(cfg);
        // We use checkpoint to trigger write operation, including instant generating and committing,
        // There can only be one checkpoint at one time.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setStateBackend(cfg.stateBackend);
        if (cfg.flinkCheckPointPath != null) {
            env.getCheckpointConfig().setCheckpointStorage(cfg.flinkCheckPointPath);
        }

        Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
        long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
        int parallelism = env.getParallelism();
        conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

        Config appConfig = ConfigService.getAppConfig();
        String[] apolloConfigKeys = cfg.apolloConfigKey.split(",");

        for (String apolloConfigKey : apolloConfigKeys) {
            // important: each hudi table must use different config object
            Configuration tableConf = conf.clone();

            String config = appConfig.getProperty(apolloConfigKey, "");
            JSONObject obj = JSONObject.parseObject(config, Feature.OrderedField);

            RowType rowType = SchemaUtils.parseTableRowType(obj.getJSONArray("fields"));

            // set apollo key to this table config key
            tableConf.setString(FlinkOptions.APOLLO_CONFIG_KEY, apolloConfigKey);

            // init kafka config
            DataStream<String> kafkaStringDataStream = initKafkaConfig(env, obj.getJSONObject("kafka_config"));
            DataStream<RowData> dataStream = kafkaStringDataStream.map(new MultiTableStringToRowDataMapFunction(apolloConfigKey));

            // init hudi config and hive sync config
            initHudiConfig(tableConf, rowType, obj.getJSONObject("hudi_config"), obj.getJSONObject("hive_sync_config"));

            writeHudi(tableConf, rowType, parallelism, dataStream);
        }

        env.execute("multiple table write to hudi");
    }

    private static DataStream<String> initKafkaConfig(StreamExecutionEnvironment env, JSONObject kafkaConfig) {

        String topic = kafkaConfig.getString("topic");
        String groupId = kafkaConfig.getString("group_id");
        String bootstrapServer = kafkaConfig.getString("bootstrap_server");

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        return env.addSource(new FlinkKafkaConsumer<>(
                        topic,
                        new SimpleStringSchema(),
                        kafkaProps
                )).name(StringUtils.join(topic, "_", groupId, "_", "kafka_source"))
                .uid(StringUtils.join(topic, "_", groupId, "_", "kafka_source_uid"));
    }

    private static void initHudiConfig(Configuration conf, RowType rowType, JSONObject hudiConfig, JSONObject hiveSyncConfig) {

        // hudi config
        conf.setString(FlinkOptions.TABLE_TYPE, hudiConfig.getString("table_type"));
        conf.setString(FlinkOptions.PATH, hudiConfig.getString("base_path"));
        conf.setString(FlinkOptions.TABLE_NAME, hudiConfig.getString("hudi_table_name"));
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, hudiConfig.getString("record_key_field"));
        conf.setString(FlinkOptions.PRECOMBINE_FIELD, hudiConfig.getString("precombine_field"));
        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, hiveSyncConfig.getString("hive_partition_field"));

        // compaction config
        conf.setString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.NUM_OR_TIME);
        conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 3);
        conf.setInteger(FlinkOptions.COMPACTION_DELTA_SECONDS, 60);

        // hive sync config
        conf.setBoolean(FlinkOptions.HIVE_SYNC_ENABLED, true);
        conf.setString(FlinkOptions.HIVE_SYNC_DB, hiveSyncConfig.getString("sync_db"));
        conf.setString(FlinkOptions.HIVE_SYNC_TABLE, hiveSyncConfig.getString("sync_table"));
        conf.setString(FlinkOptions.HIVE_SYNC_MODE, "hms");
        conf.setString(FlinkOptions.HIVE_SYNC_METASTORE_URIS, hiveSyncConfig.getString("metastore_uris"));
        conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
        conf.setString(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS, hiveSyncConfig.getString("hive_partition_field"));
        conf.setString(FlinkOptions.PARTITION_FORMAT, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);

        // set avro schema
        conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString());

        conf.setString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), conf.getString(FlinkOptions.RECORD_KEY_FIELD));
        conf.setString(FlinkOptions.INDEX_KEY_FIELD, conf.getString(FlinkOptions.RECORD_KEY_FIELD));

        // partition config
        conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, TimestampBasedAvroKeyGenerator.class.getName());
        conf.setString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), conf.getString(FlinkOptions.PARTITION_PATH_FIELD));
        conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP, TimestampBasedAvroKeyGenerator.TimestampType.DATE_STRING.name());
        conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, FlinkOptions.PARTITION_FORMAT_NORMAL);
        conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
        // fix java.lang.IllegalArgumentException: Partition path created_at=2022-08-03 is not in the form yyyy/mm/dd
        conf.setString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME, HiveStylePartitionValueExtractor.class.getCanonicalName());
    }

    private static void writeHudi(Configuration conf, RowType rowType, int parallelism, DataStream<RowData> dataStream) {
        DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream);
        DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
        if (OptionsResolver.needsAsyncCompaction(conf)) {
            Pipelines.compact(conf, pipeline);
        } else {
            Pipelines.clean(conf, pipeline);
        }

    }
}