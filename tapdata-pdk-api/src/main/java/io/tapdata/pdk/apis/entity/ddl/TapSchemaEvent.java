package io.tapdata.pdk.apis.entity.ddl;

import java.util.List;

public class TapSchemaEvent extends TapDDLEvent {
    public static final String TYPE_ADD_COLUMN = "a";
    public static final String TYPE_UPDATE_COLUMN_NAME = "un";
    public static final String TYPE_UPDATE_COLUMN_TYPE = "ut";
    public static final String TYPE_DELETE_COLUMN = "d";
    private String type;

    private List<TapField> fields;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<TapField> getFields() {
        return fields;
    }

    public void setFields(List<TapField> fields) {
        this.fields = fields;
    }
}
