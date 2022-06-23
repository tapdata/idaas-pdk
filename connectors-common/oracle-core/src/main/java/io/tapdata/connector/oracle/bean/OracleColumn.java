package io.tapdata.connector.oracle.bean;

import io.tapdata.common.CommonColumn;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

/**
 * @author Jarad
 * @date 2022/6/5
 */
public class OracleColumn extends CommonColumn {

    public OracleColumn(DataMap dataMap) {
        this.columnName = dataMap.getString("COLUMN_NAME");
        this.dataType = getDataType(dataMap); //'dataType' with precision and scale (oracle has no function)
        this.nullable = dataMap.getString("NULLABLE");
        this.remarks = dataMap.getString("COMMENTS");
        this.columnDefaultValue = dataMap.getString("DATA_DEFAULT");
    }

    @Override
    public TapField getTapField() {
        return new TapField(this.columnName, this.dataType).nullable(this.isNullable()).
                defaultValue(columnDefaultValue).comment(this.remarks);
    }

    @Override
    protected Boolean isNullable() {
        return "Y".equals(this.nullable);
    }

    private String getDataType(DataMap dataMap) {
        String dataType = dataMap.getString("DATA_TYPE");
        String dataLength = dataMap.getString("DATA_LENGTH");
        String dataPrecision = dataMap.getString("DATA_PRECISION");
        String dataScale = dataMap.getString("DATA_SCALE");
        if (dataType.contains("(")) {
            return dataType;
        } else {
            switch (dataType) {
                case "CHAR":
                case "VARCHAR2":
                case "RAW":
                    return dataType + "(" + dataLength + ")";
                case "NCHAR":
                case "NVARCHAR2":
                    return dataType + "(" + Integer.parseInt(dataLength) / 2 + ")";
                case "FLOAT":
                    return dataType + "(" + dataPrecision + ")";
                case "NUMBER":
                    if (EmptyKit.isNull(dataPrecision) && EmptyKit.isNull(dataScale)) {
                        return "NUMBER";
                    } else if (EmptyKit.isNull(dataPrecision)) {
                        return "NUMBER(*," + dataScale + ")";
                    } else {
                        return "NUMBER(" + dataPrecision + "," + dataScale + ")";
                    }
                default:
                    return dataType;
            }
        }
    }
}
