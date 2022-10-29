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

import com.beust.jcommander.JCommander;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.transform.MyRowDataToHoodieFunction;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.SchemaUtils;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.util.StringToRowDataMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility which can incrementally consume data from Kafka and apply it to the target table.
 * It has the similar functionality with SQL data source except that the source is bind to Kafka
 * and the format is bind to JSON.
 */
public class HoodieMuiltiTableFlinkStreamer {

  private static final Logger LOG = LoggerFactory.getLogger(MyRowDataToHoodieFunction.class);

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

    TypedProperties kafkaProps = DFSPropertiesConfiguration.getGlobalProps();
    kafkaProps.putAll(StreamerUtil.appendKafkaProps(cfg));

    // get rowtype from apollo config
    Config appConfig = ConfigService.getAppConfig();
    String schema = appConfig.getProperty(cfg.apolloConfigKey, "");
    LOG.info("apollo schema: {}", schema);

    RowType rowType = SchemaUtils.parseTableRowType(schema);

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    int parallelism = env.getParallelism();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

    // set avro schema
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(rowType).toString());

    DataStream<String> kafkaStringDataStream = env.addSource(new FlinkKafkaConsumer<>(
                    cfg.kafkaTopic,
                    new SimpleStringSchema(),
                    kafkaProps
            )).name("kafka_source")
            .uid("uid_kafka_source");

    DataStream<RowData> dataStream = kafkaStringDataStream.map(new StringToRowDataMapFunction(cfg.apolloConfigKey));

    if (cfg.transformerClassNames != null && !cfg.transformerClassNames.isEmpty()) {
      Option<Transformer> transformer = StreamerUtil.createTransformer(cfg.transformerClassNames);
      if (transformer.isPresent()) {
        dataStream = transformer.get().apply(dataStream);
      }
    }

    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
    if (OptionsResolver.needsAsyncCompaction(conf)) {
      Pipelines.compact(conf, pipeline);
    } else {
      Pipelines.clean(conf, pipeline);
    }

    env.execute(cfg.targetTableName);
  }
}
