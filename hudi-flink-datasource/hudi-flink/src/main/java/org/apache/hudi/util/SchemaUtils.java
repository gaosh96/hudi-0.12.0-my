package org.apache.hudi.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;

/**
 * @author gaosh
 * @version 1.0
 * @since 10/19/22
 */
public class SchemaUtils {

    private static final String DECIMAL_PATTERN = "decimal\\((\\d+),(\\d+)";


    public static RowType parseTableRowType(String schema) {
        JSONObject fields = JSON.parseObject(schema, Feature.OrderedField);
        String[] fieldNames = fields.keySet().toArray(new String[]{});
        LogicalType[] logicalTypes = new LogicalType[fieldNames.length];

        for (int i = 0; i < fieldNames.length; i++) {
            String type = fields.getString(fieldNames[i]);
            logicalTypes[i] = getFieldLogicalType(type.toLowerCase());
        }

        // set nullable to false
        return RowType.of(false, logicalTypes, fieldNames);
    }

    private static LogicalType getFieldLogicalType(String type) {

        if (type.contains("varchar") || type.equals("string")) return new VarCharType();

        if (type.contains("decimal")) {
            Tuple2<Integer, Integer> decimalInfo = getDecimalInfo(type);
            return new DecimalType(decimalInfo.f0, decimalInfo.f1);
        }

        if (type.contains("timestamp")) {
            return new TimestampType(0);
        }

        switch (type) {
            case "int":
                return new IntType();
            case "bigint":
                return new BigIntType();
            default:
                throw new RuntimeException("ERROR TYPE");
        }

    }

    /**
     * 解析 decimal 类型的精度
     *
     * @param decimalType
     * @return
     */
    public static Tuple2<Integer, Integer> getDecimalInfo(String decimalType) {
        Pattern pattern = Pattern.compile(DECIMAL_PATTERN);
        Matcher matcher = pattern.matcher(decimalType);

        if (matcher.find()) {
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));

            return Tuple2.of(precision, scale);
        }
        return Tuple2.of(38, 10);
    }

    public static TimestampData convertToTimestamp(String ts) {
        TemporalAccessor parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(ts);

        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

}
