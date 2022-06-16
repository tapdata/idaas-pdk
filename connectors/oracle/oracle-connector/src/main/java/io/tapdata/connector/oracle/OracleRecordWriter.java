package io.tapdata.connector.oracle;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class OracleRecordWriter extends RecordWriter {

    public OracleRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new OracleWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new OracleWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new OracleWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}
