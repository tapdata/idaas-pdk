package io.tapdata.pdk.apis.entity;

import io.tapdata.pdk.apis.entity.ddl.TapSchemaEvent;
import io.tapdata.pdk.apis.entity.ddl.TapTableEvent;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SupportedTapEvents {
    private List<String> supportedDMLTypes = new ArrayList<>(Arrays.asList(
            TapRecordEvent.TYPE_DELETE,
            TapRecordEvent.TYPE_INSERT,
            TapRecordEvent.TYPE_TRANSACTION_COMMIT,
            TapRecordEvent.TYPE_TRANSACTION_START,
            TapRecordEvent.TYPE_UPDATE
    ));
    private List<String> supportedTableTypes = new ArrayList<>(Arrays.asList(
            TapTableEvent.TYPE_ALTER_TABLE_NAME,
            TapTableEvent.TYPE_CLEAR_TABLE,
            TapTableEvent.TYPE_CREATE_TABLE,
            TapTableEvent.TYPE_DROP_TABLE
    ));
    private List<String> supportedSchemaTypes = new ArrayList<>(Arrays.asList(
            TapSchemaEvent.TYPE_ADD_COLUMN,
            TapSchemaEvent.TYPE_DELETE_COLUMN,
            TapSchemaEvent.TYPE_UPDATE_COLUMN_NAME,
            TapSchemaEvent.TYPE_UPDATE_COLUMN_TYPE
    ));

    public SupportedTapEvents supportTableTypes(List<String> supportedTableTypes) {
        this.supportedTableTypes = supportedTableTypes;
        return this;
    }
    public SupportedTapEvents supportSchemaTypes(List<String> supportedSchemaTypes) {
        this.supportedSchemaTypes = supportedSchemaTypes;
        return this;
    }
    public SupportedTapEvents supportDMLTypes(List<String> supportedDMLTypes) {
        this.supportedDMLTypes = supportedDMLTypes;
        return this;
    }

    public SupportedTapEvents notSupportTableTypes() {
        if(supportedTableTypes != null)
            supportedTableTypes.clear();
        return this;
    }
    public SupportedTapEvents notSupportSchemaTypes() {
        if(supportedSchemaTypes != null)
            supportedSchemaTypes.clear();
        return this;
    }
    public List<String> getSupportedDMLTypes() {
        return supportedDMLTypes;
    }

    public void setSupportedDMLTypes(List<String> supportedDMLTypes) {
        this.supportedDMLTypes = supportedDMLTypes;
    }

    public List<String> getSupportedTableTypes() {
        return supportedTableTypes;
    }

    public void setSupportedTableTypes(List<String> supportedTableTypes) {
        this.supportedTableTypes = supportedTableTypes;
    }

    public List<String> getSupportedSchemaTypes() {
        return supportedSchemaTypes;
    }

    public void setSupportedSchemaTypes(List<String> supportedSchemaTypes) {
        this.supportedSchemaTypes = supportedSchemaTypes;
    }
}
