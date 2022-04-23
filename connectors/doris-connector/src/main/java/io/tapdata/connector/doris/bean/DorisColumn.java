package io.tapdata.connector.doris.bean;

import io.tapdata.base.bean.CommonColumn;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DorisColumn extends CommonColumn {

    private String tableCatalog;
    private String tableSchema;
    private String tableName;
    private String typeName;
    private String columnSize;
    private String decimalDigits;
    private String numPrecisionRadix;
    private String charOctetLength;
    private String ordinalPosition;
    private String sourceDataType;
    private String isAutoincrement;

    private DorisColumn() {
    }

    public DorisColumn(ResultSet resultSet) throws SQLException {
        this.tableCatalog = resultSet.getString("TABLE_CAT");
        this.tableSchema = resultSet.getString("TABLE_SCHEM");
        this.tableName = resultSet.getString("TABLE_NAME");
        this.columnName = resultSet.getString("COLUMN_NAME");
        this.dataType = resultSet.getString("DATA_TYPE");
        this.typeName = resultSet.getString("TYPE_NAME");
        this.columnSize = resultSet.getString("COLUMN_SIZE");

        this.decimalDigits = resultSet.getString("DECIMAL_DIGITS");
        this.numPrecisionRadix = resultSet.getString("NUM_PREC_RADIX");
        this.nullable = resultSet.getString("NULLABLE");
        this.remarks = resultSet.getString("REMARKS");
        this.columnDefaultValue = resultSet.getString("COLUMN_DEF");

        this.charOctetLength = resultSet.getString("CHAR_OCTET_LENGTH");
        this.ordinalPosition = resultSet.getString("ORDINAL_POSITION");

        this.sourceDataType = resultSet.getString("SOURCE_DATA_TYPE");
        this.isAutoincrement = resultSet.getString("IS_AUTOINCREMENT");
    }

}
