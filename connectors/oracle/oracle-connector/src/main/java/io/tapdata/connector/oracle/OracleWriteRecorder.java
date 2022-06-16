package io.tapdata.connector.oracle;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class OracleWriteRecorder extends WriteRecorder {

    public OracleWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {

    }

    @Override
    public void addUpdateBatch(Map<String, Object> after) throws SQLException {

    }

    @Override
    public void addDeleteBatch(Map<String, Object> before) throws SQLException {

    }
}
