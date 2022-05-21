package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * make sql
 *
 * @author Jarad
 * @date 2022/4/29
 */
public class PostgresSqlMaker {

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
            builder.append('\"').append(tapField.getName()).append("\" ").append(tapField.getDataType()).append(' ');
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
        return "INSERT INTO \"" + tapTable.getId() + "\" VALUES (" + stringBuilder + ")";
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
        if (EmptyKit.isEmpty(insertRecord)) {
            return;
        }
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
                preparedStatement.setObject(pos, tapValue);
            }
            pos += 1;
        }
        preparedStatement.addBatch();
    }

    /**
     * build subSql after where for advance query
     *
     * @param filter condition of advance query
     * @return where substring
     */
    public static String buildSqlByAdvanceFilter(TapAdvanceFilter filter) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(filter.getMatch()) || EmptyKit.isNotEmpty(filter.getOperators())) {
            builder.append("WHERE ");
            builder.append(PostgresSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "="));
        }
        if (EmptyKit.isNotEmpty(filter.getOperators())) {
            if (EmptyKit.isNotEmpty(filter.getMatch())) {
                builder.append("AND ");
            }
            builder.append(filter.getOperators().stream().map(v -> v.toString("\"")).reduce((v1, v2) -> v1 + " AND " + v2).orElseGet(String::new)).append(' ');
        }
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString("\"")).reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new)).append(' ');
        }
        if (null != filter.getSkip()) {
            builder.append("OFFSET ").append(filter.getSkip()).append(' ');
        }
        if (null != filter.getLimit()) {
            builder.append("LIMIT ").append(filter.getLimit()).append(' ');
        }
        return builder.toString();
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
    public static String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                builder.append('\"').append(fieldName).append('\"').append(operator);
                if (!(value instanceof Number)) {
                    builder.append('\'').append(value).append('\'');
                } else {
                    builder.append(value);
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
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
}
