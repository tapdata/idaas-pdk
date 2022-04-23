package io.tapdata.connector.mysql.bean;

import io.tapdata.base.bean.CommonColumn;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class Mysql5Column extends CommonColumn {

    public Mysql5Column(ResultSet resultSet) throws SQLException {
//        this.columnName = resultSet.getString("COLUMN_NAME");
//        this.remarks = resultSet.getString("COLUMN_COMMENT");
//        this.dataType = resultSet.getString("DATA_TYPE");
//        this.columnDefaultValue = resultSet.getString("COLUMN_DEFAULT");
//        this.nullable = resultSet.getString("IS_NULLABLE");

        this.columnName = resultSet.getString("COLUMN_NAME");
        this.dataType = resultSet.getString("DATA_TYPE");
        this.nullable = resultSet.getString("NULLABLE");
        this.remarks = resultSet.getString("REMARKS");
        this.columnDefaultValue = resultSet.getString("COLUMN_DEF");
    }
}
