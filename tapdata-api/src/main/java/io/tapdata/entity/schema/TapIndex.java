package io.tapdata.entity.schema;

import java.io.Serializable;
import java.util.List;

public class TapIndex implements Serializable {
    /**
     * Index name
     */
    private String name;
    /**
     * Index fields
     */
    private List<TapIndexField> indexFields;

    private boolean unique;

    private boolean primary;

    public List<TapIndexField> getIndexFields() {
        return indexFields;
    }

    public void setIndexFields(List<TapIndexField> indexFields) {
        this.indexFields = indexFields;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
