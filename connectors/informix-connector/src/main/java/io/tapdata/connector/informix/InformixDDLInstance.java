package io.tapdata.connector.informix;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class InformixDDLInstance {
    private static final InformixDDLInstance DDLInstance = new InformixDDLInstance();

    public static InformixDDLInstance getInstance() {
        return DDLInstance;
    }

    public String buildDistributedKey(Collection<String> primaryKeyNames) {
        StringBuilder builder = new StringBuilder();
        for (String fieldName : primaryKeyNames) {
            builder.append(fieldName);
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    public String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getOriginType() == null) continue;
            builder.append(tapField.getName()).append(' ');
            builder.append(tapField.getOriginType()).append(' ');
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            } else {
                builder.append("NULL").append(' ');
            }
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ').append(tapField.getDefaultValue()).append(' ');
            }
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    public static String buildBatchInsertSQL(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int fieldCount = 0;
        for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            TapField tapField = nameFieldMap.get(entry.getKey());
            if (tapField.getOriginType() == null) continue;
            fieldCount += 1;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fieldCount; i++) {
            stringBuilder.append("?, ");
        }
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return "INSERT INTO " + tapTable.getName() + " VALUES (" + stringBuilder + ")";
    }

}
