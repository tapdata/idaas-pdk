package io.tapdata.connector.postgres;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
     * id text ,
     * tapString text NOT NULL ,
     * tddUser text ,
     * tapString10 VARCHAR(10) NOT NULL
     *
     * @param tapTable Table Object
     * @return substring of SQL
     */
    public static String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v -> v.getValue().getPos())).map(v -> {
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            if (tapField.getDataType() == null) {
                return "";
            }
            builder.append('\"').append(tapField.getName()).append("\" ").append(tapField.getDataType()).append(' ');
            //null to omit
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            }
            //null to omit
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ');
                if (tapField.getDefaultValue() instanceof Number) {
                    builder.append(tapField.getDefaultValue()).append(' ');
                } else {
                    builder.append("'").append(tapField.getDefaultValue()).append("' ");
                }
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
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
            builder.append(filter.getOperators().stream().map(v -> v.toString("\"")).collect(Collectors.joining(" AND "))).append(' ');
        }
        if (EmptyKit.isNotEmpty(filter.getSortOnList())) {
            builder.append("ORDER BY ");
            builder.append(filter.getSortOnList().stream().map(v -> v.toString("\"")).collect(Collectors.joining(", "))).append(' ');
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

}
