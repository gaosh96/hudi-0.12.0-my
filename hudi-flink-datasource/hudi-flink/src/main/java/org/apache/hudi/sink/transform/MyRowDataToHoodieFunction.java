/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.transform;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.SchemaUtils;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;

/**
 * Function that transforms RowData to HoodieRecord.
 */
public class MyRowDataToHoodieFunction<I extends RowData, O extends HoodieRecord>
    extends RichMapFunction<I, O> {

  private static final Logger LOG = LoggerFactory.getLogger(MyRowDataToHoodieFunction.class);

  /**
   * Row type of the input.
   */
  private RowType rowType;

  /**
   * Avro schema of the input.
   */
  private transient Schema avroSchema;

  /**
   * RowData to Avro record converter.
   */
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;

  /**
   * HoodieKey generator.
   */
  private transient KeyGenerator keyGenerator;

  /**
   * Utilities to create hoodie pay load instance.
   */
  private transient PayloadCreation payloadCreation;

  /**
   * Config options.
   */
  private final Configuration config;

  public MyRowDataToHoodieFunction(RowType rowType, Configuration config) {
    this.rowType = rowType;
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType);
    this.keyGenerator =
        HoodieAvroKeyGeneratorFactory
            .createKeyGenerator(flinkConf2TypedProperties(this.config));
    this.payloadCreation = PayloadCreation.instance(config);

    if (config.getBoolean(FlinkOptions.APOLLO_CONFIG_ENABLED)) {
      Config appConfig = ConfigService.getConfig(config.getString(FlinkOptions.APOLLO_CONFIG_NAMESPACE));
      String apolloConfigKey = config.getString(FlinkOptions.APOLLO_CONFIG_KEY);

      appConfig.addChangeListener(event -> {
        if (event.isChanged(apolloConfigKey)) {
          String schema = appConfig.getProperty(apolloConfigKey, "");

          // for multiple table
          JSONObject obj = JSONObject.parseObject(schema, Feature.OrderedField);
          JSONArray fields = obj.getJSONArray("fields");

          this.rowType = SchemaUtils.parseTableRowType(fields);
          this.avroSchema = AvroSchemaConverter.convertToSchema(rowType);
          this.config.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, avroSchema.toString());
          this.converter = RowDataToAvroConverters.createConverter(rowType);
        }
      });
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public O map(I i) throws Exception {
    return (O) toHoodieRecord(i);
  }

  /**
   * Converts the give record to a {@link HoodieRecord}.
   *
   * @param record The input record
   * @return HoodieRecord based on the configuration
   * @throws IOException if error occurs
   */
  @SuppressWarnings("rawtypes")
  private HoodieRecord toHoodieRecord(I record) throws Exception {
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);

    HoodieRecordPayload payload = payloadCreation.createPayload(gr);
    HoodieOperation operation = HoodieOperation.fromValue(record.getRowKind().toByteValue());
    return new HoodieAvroRecord<>(hoodieKey, payload, operation);
  }
}
