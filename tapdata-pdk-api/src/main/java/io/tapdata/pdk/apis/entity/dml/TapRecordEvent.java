package io.tapdata.pdk.apis.entity.dml;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ddl.FieldConvertor;
import io.tapdata.pdk.apis.entity.ddl.TapField;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.entity.dml.TapDMLEvent;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class TapRecordEvent extends TapDMLEvent {
    public static final String TYPE_INSERT = "i";
//    public static final String TYPE_UPSERT = "ui";
    public static final String TYPE_UPDATE = "u";
    public static final String TYPE_DELETE = "d";

    //TODO transaction events
    public static final String TYPE_TRANSACTION_START = "ts";
    public static final String TYPE_TRANSACTION_COMMIT = "tc";
    /**
     * The record event type, whether insert, update or delete
     */
    private String type;

    /**
     * The latest record, after insert and update
     * Value format should follow TapType formats
     */
    private Map<String, Object> after;
    /**
     * The last record, especially before update and delete
     * Value format should follow TapType formats
     */
    private Map<String, Object> before;

    /**
     * Record the special fields that can not be converted to TapType format, but want to keep the origin format, in case saving to the same data source.
     * If processor touch the value of the origin field, the field should be removed as value is changed.
     * If the record flow into different data source, this map should be cleared.
     */
    private Map<String, Object> originFieldMap;
    /**
     * Table name of the record
     */
    private String tableName;


    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getValue() {
        if (after != null)
            return after;
        return before;
    }

    public void setValue(Map<String, Object> value) {
        after = value;
    }

    public Map<String, Object> getOriginFieldMap() {
        return originFieldMap;
    }

    public void setOriginFieldMap(Map<String, Object> originFieldMap) {
        this.originFieldMap = originFieldMap;
    }

    public static TapRecordEvent create(String type, TapConnectorContext connectorContext) {
        if(connectorContext == null)
            throw new IllegalArgumentException("Create record event failed, connectorContext is null");
        if(type == null)
            throw new IllegalArgumentException("Create record event failed, type is null");
        TapTable table = connectorContext.getTable();
        if(table == null)
            throw new IllegalArgumentException("Create record event failed, table is null");
        switch (type) {
            case TYPE_DELETE:
            case TYPE_INSERT:
            case TYPE_TRANSACTION_COMMIT:
            case TYPE_TRANSACTION_START:
            case TYPE_UPDATE:
                break;
            default:
                throw new IllegalArgumentException("Create record event failed, type illegal " + type);
        }
        TapRecordEvent tapRecordEvent = new TapRecordEvent();
        tapRecordEvent.setType(type);
        tapRecordEvent.setTime(System.currentTimeMillis());
        tapRecordEvent.setTableName(table.getName());
        tapRecordEvent.setPdkId(connectorContext.getSpecification().getId());
        tapRecordEvent.setPdkGroup(connectorContext.getSpecification().getGroup());
        return tapRecordEvent;
    }

    public Map<String, Object> formatValue(Map<String, Object> record, Map<String, TapField> fieldMap) {
        if(fieldMap == null)
            return record;
        Collection<TapField> fields = fieldMap.values();
        for(TapField field : fields) {
            Object value = record.get(field.getName());
            if(value != null) {
                FieldConvertor fieldConvertor = field.getOriginConvertor();
                if(fieldConvertor != null) {
                    Object newValue = fieldConvertor.convert(value);
                    if(newValue != null) {
                        if(originFieldMap == null) {
                            originFieldMap = new LinkedHashMap<>();
                        }
                        originFieldMap.put(field.getName(), value);
                        record.put(field.getName(), newValue);
                    }
                }
            }
        }
        return record;
    }
}
