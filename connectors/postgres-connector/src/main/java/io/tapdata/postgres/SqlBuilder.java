package io.tapdata.postgres;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * make sql
 *
 * @author Jarad
 * @date 2022/4/29
 */
public class SqlBuilder {

    /**
     * combine column definition for creating table
     * e.g.
     * id text NULL ,
     * tapString text NULL ,
     * tddUser text NULL ,
     * tapString10 VARCHAR(10) NULL
     *
     * @param tapTable Table Object
     * @return substring of SQL
     */
    public static String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        nameFieldMap.keySet().forEach(columnName -> {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getDataType() == null) {
                return;
            }
            builder.append(tapField.getName()).append(' ').append(tapField.getDataType()).append(' ');
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            } else {
                builder.append("NULL").append(' ');
            }
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ').append(tapField.getDefaultValue()).append(' ');
            }
            builder.append(',');
        });
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    /**
     * combine columns for inserting records
     * e.g.
     * INSERT INTO DMLTest_postgres_oFsAOk VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     *
     * @param tapTable Table Object
     * @return insert SQL
     */
    public static String buildPrepareInsertSQL(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder stringBuilder = new StringBuilder();
        long fieldCount = nameFieldMap.keySet().stream().filter(v -> null != nameFieldMap.get(v).getDataType()).count();
        for (int i = 0; i < fieldCount; i++) {
            stringBuilder.append("?, ");
        }
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return "INSERT INTO " + tapTable.getName() + " VALUES (" + stringBuilder + ")";
    }

    /**
     * build prepareStatement with one record
     *
     * @param tapTable          table
     * @param insertRecord      one record
     * @param preparedStatement ps
     * @throws SQLException SQLException
     */
    public static void addBatchInsertRecord(TapTable tapTable, Map<String, Object> insertRecord, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.clearParameters();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int pos = 1;
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = insertRecord.get(columnName);
            if (tapField.getDataType() == null) {
                continue;
            }
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    preparedStatement.setObject(pos, tapField.getDefaultValue());
                } else {
                    preparedStatement.setObject(pos, null);
                }
            } else {
                preparedStatement.setObject(pos, getFieldOriginValue(tapValue));
            }
            pos += 1;
        }
        preparedStatement.addBatch();
    }

    /**
     * set value for each column in sql
     * e.g.
     * id=12,name=Jarad,age=34
     *
     * @param record      key-val
     * @param splitSymbol split symbol
     * @return substring of sql
     */
    public static String buildKeyAndValue(Map<String, Object> record, String splitSymbol) {
        StringBuilder builder = new StringBuilder();
        record.forEach((fieldName, value) -> {
            builder.append(fieldName).append("=");
            if (!(value instanceof Number)) {
                builder.append("'").append(getFieldOriginValue(value)).append("'");
            } else {
                builder.append(getFieldOriginValue(value));
            }
            builder.append(splitSymbol).append(" ");
        });
        builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        return builder.toString();
    }

    /**
     * public String buildInsertValues(TapTable tapTable, Map<String, Object> record) {
     * LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
     * StringBuilder builder = new StringBuilder();
     * for (String columnName : nameFieldMap.keySet()) {
     * TapField tapField = nameFieldMap.get(columnName);
     * Object tapValue = record.get(columnName);
     * if (tapField.getDataType() == null) continue;
     * if (tapValue == null) {
     * if (tapField.getNullable() != null && !tapField.getNullable()) {
     * builder.append("'").append(tapField.getDefaultValue()).append("'").append(',');
     * } else {
     * builder.append("null").append(',');
     * }
     * } else {
     * builder.append("'").append(getFieldOriginValue(tapValue)).append("'").append(',');
     * }
     * }
     * builder.delete(builder.length() - 1, builder.length());
     * return builder.toString();
     * }
     */

    private static Object getFieldOriginValue(Object tapValue) {
        if (tapValue instanceof DateTime) {
            DateTime dateTime = (DateTime) tapValue;
            return new java.sql.Date(dateTime.getSeconds() * 1000L + dateTime.getNano() / 1000000L);
        } else if (tapValue instanceof Date) {
            return new java.sql.Date(((Date) tapValue).getTime());
        }
        return tapValue;
    }
}
