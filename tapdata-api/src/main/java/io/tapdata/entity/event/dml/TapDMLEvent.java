package io.tapdata.entity.event.dml;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;

public class TapDMLEvent extends TapEvent {
    /**
     * 数据源的类型， mysql一类
     */
    private String connector;
    /**
     * 数据源的版本
     */
    private String connectorVersion;

    /**
     * Table name of the record
     */
    protected TapTable table;

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getConnectorVersion() {
        return connectorVersion;
    }

    public void setConnectorVersion(String connectorVersion) {
        this.connectorVersion = connectorVersion;
    }

//    public Map<String, Object> formatValue(Map<String, Object> record, Map<String, TapField> fieldMap) {
//        if(fieldMap == null)
//            return record;
//        Collection<TapField> fields = fieldMap.values();
//        for(TapField field : fields) {
//            Object value = record.get(field.getName());
//            if(value != null) {
//                FieldConvertor fieldConvertor = field.getOriginConverter();
//                if(fieldConvertor != null) {
//                    TapValue<?, ?> newValue = fieldConvertor.convert(value);
//                    if(newValue != null) {
//                        newValue.setOriginValue(value);
//                        newValue.setOriginValue(field.getName());
//                        record.put(field.getName(), newValue);
//                    }
//                }
//            }
//        }
//        return record;
//    }
}
