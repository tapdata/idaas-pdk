package io.tapdata.postgres.bean;

import io.tapdata.entity.schema.TapField;

/**
 * attributes for common columns
 *
 * @author Jarad
 * @date 2022/4/20
 */
public class CommonColumn {

    protected String columnName;
    protected String dataType;
    protected String nullable;
    protected String remarks;
    protected String columnDefaultValue;

    public CommonColumn() {
    }

    private Boolean isNullable() {
        return "1".equals(this.nullable);
    }

    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    public String getColumnName() {
        return columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public String getNullable() {
        return nullable;
    }

    public String getRemarks() {
        return remarks;
    }

    public String getColumnDefaultValue() {
        return columnDefaultValue;
    }
}
