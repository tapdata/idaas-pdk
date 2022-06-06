package io.tapdata.connector.postgres.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class PostgresColumn extends CommonColumn {

    public PostgresColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("column_name");
        this.dataType = dataMap.getString("dataType"); //'dataType' with precision and scale (postgres has its function)
//        this.dataType = dataMap.getString("data_type"); //'data_type' without precision or scale
        this.nullable = dataMap.getString("is_nullable");
        this.remarks = dataMap.getString("remark");
        this.columnDefaultValue = dataMap.getString("column_default");
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "YES".equals(this.nullable);
    }
}
