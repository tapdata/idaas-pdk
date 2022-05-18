package io.tapdata.connector.postgres.bean;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Jarad
 * @date 2022/4/20
 */
public class PostgresColumn extends CommonColumn {

    public PostgresColumn(ResultSet resultSet) throws SQLException {
        this.columnName = resultSet.getString("COLUMN_NAME");
        this.dataType = JDBCType.valueOf(resultSet.getInt("DATA_TYPE")).getName();
        this.nullable = resultSet.getString("NULLABLE");
        this.remarks = resultSet.getString("REMARKS");
        this.columnDefaultValue = resultSet.getString("COLUMN_DEF");
    }
}
