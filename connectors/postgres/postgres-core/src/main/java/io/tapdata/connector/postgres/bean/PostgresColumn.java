package io.tapdata.connector.postgres.bean;

import io.tapdata.entity.utils.DataMap;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class PostgresColumn extends CommonColumn {

    public PostgresColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("column_name");
        this.dataType = dataMap.getString("data_type");
        this.nullable = dataMap.getString("is_nullable");
        this.remarks = dataMap.getString("remark");
        this.columnDefaultValue = dataMap.getString("column_default");
    }
}
