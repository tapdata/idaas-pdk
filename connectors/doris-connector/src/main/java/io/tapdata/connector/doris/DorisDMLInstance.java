package io.tapdata.connector.doris;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class DorisDMLInstance {
    private final SimpleDateFormat tapDateTimeFormat = new SimpleDateFormat();
    private static final DorisDMLInstance DMLInstance = new DorisDMLInstance();

    public static DorisDMLInstance getInstance(){
        return DMLInstance;
    }

    private String formatTapDateTime(DateTime dateTime, String pattern) {
        if (dateTime.getTimeZone() != null) dateTime.setTimeZone(dateTime.getTimeZone());
        tapDateTimeFormat.applyPattern(pattern);
        return tapDateTimeFormat.format(new Date(dateTime.getSeconds() * 1000L));
    }

    private String formatTapDateTime(Date date, String pattern) {
        tapDateTimeFormat.applyPattern(pattern);
        return tapDateTimeFormat.format(date);
    }

    public Object getFieldOriginValue(TapField tapField, Object tapValue) {
        Object result = tapValue;
        if (tapValue instanceof DateTime) {
            result = formatTapDateTime((DateTime) tapValue, "yyyy-MM-dd HH:mm:ss");
        } else if(tapValue instanceof Date) {
            result = formatTapDateTime((Date) tapValue, "yyyy-MM-dd HH:mm:ss");
        }
        return result;
    }

    public void addBatchInsertRecord(TapTable tapTable, Map<String, Object> insertRecord, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.clearParameters();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int pos = 1;
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = insertRecord.get(columnName);
            if (tapField.getDataType() == null) continue;
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    preparedStatement.setObject(pos, tapField.getDefaultValue());
                } else {
                    preparedStatement.setObject(pos, null);
                }
            } else {
                preparedStatement.setObject(pos, getFieldOriginValue(tapField, tapValue));
            }
            pos += 1;
        }
        preparedStatement.addBatch();
    }

    public String buildBatchInsertSQL(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int fieldCount = 0;
        for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            TapField tapField = nameFieldMap.get(entry.getKey());
            if (tapField.getDataType() == null) continue;
            fieldCount += 1;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fieldCount; i++) {
            stringBuilder.append("?, ");
        }
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return "INSERT INTO " + tapTable.getName() + " VALUES (" + stringBuilder + ")";
    }

    public String buildKeyAndValue(TapTable tapTable, Map<String, Object> record, String splitSymbol) {
        StringBuilder builder = new StringBuilder();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String fieldName = entry.getKey();
            builder.append(fieldName).append("=");
            if(!(entry.getValue() instanceof Number))
                builder.append("'");

            builder.append(getFieldOriginValue(nameFieldMap.get(fieldName), entry.getValue()));

            if(!(entry.getValue() instanceof Number))
                builder.append("'");

            builder.append(splitSymbol).append(" ");
        }
        builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        return builder.toString();
    }

    public String buildValues(TapTable tapTable, Map<String, Object> record) {
        // 之前作为单条记录插入使用 insert into table values ([buildInsertKeyAndValues])
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = record.get(columnName);
            if (tapField.getDataType() == null) continue;
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    builder.append("\'").append(tapField.getDefaultValue()).append("'").append(',');
                } else {
                    builder.append("null").append(',');
                }
            } else {
                builder.append("'").append(getFieldOriginValue(tapField, tapValue)).append("'").append(',');
            }
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }
}
