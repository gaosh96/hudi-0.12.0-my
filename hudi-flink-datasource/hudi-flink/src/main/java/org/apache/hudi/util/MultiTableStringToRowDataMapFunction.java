package org.apache.hudi.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gaosh
 * @version 1.0
 * @since 10/19/22
 */
public class MultiTableStringToRowDataMapFunction extends RichMapFunction<String, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableStringToRowDataMapFunction.class);

    private final String schemaApolloConfigKey;

    private String[] fieldNames;

    private List<String> fieldTypes;

    public MultiTableStringToRowDataMapFunction(String schemaApolloConfigKey) {
        this.schemaApolloConfigKey = schemaApolloConfigKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Config appConfig = ConfigService.getAppConfig();
        String tableConfig = appConfig.getProperty(schemaApolloConfigKey, "");
        handleFieldInfo(tableConfig);

        appConfig.addChangeListener(new ConfigChangeListener() {
            @Override
            public void onChange(ConfigChangeEvent event) {
                if (event.isChanged(schemaApolloConfigKey)) {
                    String schema = appConfig.getProperty(schemaApolloConfigKey, "");
                    // update fieldNames and fieldTypes
                    handleFieldInfo(schema);

                    LOG.info("update fieldNames: {}", Arrays.toString(fieldNames));
                    LOG.info("update fieldTypes: {}", fieldTypes);
                }
            }
        });
    }

    @Override
    public RowData map(String value) throws Exception {

        GenericRowData rowData = new GenericRowData(fieldNames.length);
        JSONObject record = JSON.parseObject(value, Feature.OrderedField);

        // op
        String op = record.getString("op");
        setRecordRowKind(rowData, op);

        for (int i = 0; i < fieldNames.length; i++) {
            setFieldValue(rowData, record, i, fieldNames[i], fieldTypes.get(i));
        }

        LOG.info("rowData: {}", rowData);
        return rowData;
    }

    private void setFieldValue(GenericRowData rowData, JSONObject record, int index, String fieldName, String fieldType) {

        if (fieldType.contains("varchar") || fieldType.equals("string")) {
            rowData.setField(index, StringData.fromString(record.getString(fieldName)));
        }

        if (fieldType.contains("decimal")) {
            Tuple2<Integer, Integer> decimalInfo = SchemaUtils.getDecimalInfo(fieldType);
            rowData.setField(index, DecimalData.fromBigDecimal(record.getBigDecimal(fieldName), decimalInfo.f0, decimalInfo.f1));
        }

        if (fieldType.contains("timestamp")) {
            rowData.setField(index, SchemaUtils.convertToTimestamp(record.getString(fieldName)));
        }

        switch (fieldType) {
            case "int":
                rowData.setField(index, record.getIntValue(fieldName));
                break;
            case "bigint":
                rowData.setField(index, record.getLongValue(fieldName));
                break;
        }

    }

    private void setRecordRowKind(GenericRowData rowData, String op) {
        switch (op) {
            case "I":
                rowData.setRowKind(RowKind.INSERT);
                break;
            case "U":
                rowData.setRowKind(RowKind.UPDATE_AFTER);
                break;
            case "D":
                rowData.setRowKind(RowKind.DELETE);
                break;
        }
    }

    private void handleFieldInfo(String tableConfig) {
        JSONArray fields = JSON.parseObject(tableConfig, Feature.OrderedField).getJSONArray("fields");




        fieldNames = fields.keySet().toArray(new String[]{});
        fieldTypes = fields.values().stream().map(String::valueOf).collect(Collectors.toList());
    }

}
