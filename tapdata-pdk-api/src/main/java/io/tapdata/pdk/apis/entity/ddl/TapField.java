package io.tapdata.pdk.apis.entity.ddl;

import io.tapdata.pdk.apis.common.DefaultMap;

import java.util.Map;
import java.util.Properties;

/**
 * @author dxtr
 */
public class TapField {
    private String name;
    /**
     *
     */
    private Integer precision;
    /**
     *
     */
    private Integer scale;
    /**
     *
     */
    private boolean autoincrement;
    /**
     *
     */
    private boolean nullable;
    /**
     *
     */
    private Object defaultValue;
    /**
     *
     */
    private int columnPosition;
    /**
     *
     */
    private String tapType;
    /**
     *
     */
    private Map<String, Object> properties;
    /**
     * 
     */
    private String originalType;

    /**
     * Convert origin value to TapType value.
     * This will only be used for the value not compatible with TapType, need manual conversion.
     */
    private FieldConvertor originConvertor;

    public FieldConvertor getOriginConvertor() {
        return originConvertor;
    }

    public void setOriginConvertor(FieldConvertor originConvertor) {
        this.originConvertor = originConvertor;
    }

    public String getOriginalType() {
        return originalType;
    }

    public void setOriginalType(String originalType) {
        this.originalType = originalType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public boolean isAutoincrement() {
        return autoincrement;
    }

    public void setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getColumnPosition() {
        return columnPosition;
    }

    public void setColumnPosition(int columnPosition) {
        this.columnPosition = columnPosition;
    }

    public String getTapType() {
        return tapType;
    }

    public void setTapType(String tapType) {
        this.tapType = tapType;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
