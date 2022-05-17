package io.tapdata.entity.schema;

import java.io.Serializable;
import java.util.List;

public class TapIndexField implements Serializable {
    /**
     * Index name
     */
    private String name;
    /**
     * Field is asc or not.
     */
    private Boolean fieldAsc;

    public Boolean getFieldAsc() {
        return fieldAsc;
    }

    public void setFieldAsc(Boolean fieldAsc) {
        this.fieldAsc = fieldAsc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
