package io.tapdata.entity.schema;

import java.util.List;

public class TapIndex {
    /**
     * Index name
     */
    private String name;
    /**
     * Index fields
     */
    private List<String> fields;
    /**
     * Asc list should be the same size of fields. or null.
     * Each item stand for each field is asc or not.
     */
    private List<Boolean> fieldAscList;

    private boolean unique;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<Boolean> getFieldAscList() {
        return fieldAscList;
    }

    public void setFieldAscList(List<Boolean> fieldAscList) {
        this.fieldAscList = fieldAscList;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
