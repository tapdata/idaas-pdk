package io.tapdata.postgres;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.postgres.bean.PostgresColumn;
import io.tapdata.postgres.bean.PostgresConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_postgres.json")
public class PostgresConnector extends ConnectorBase implements TapConnector {

    private PostgresConfig postgresConfig;
    private Connection conn;
    private Statement stmt;
    private static final String TABLE_COLUMN_NAME = "TABLE";

    private void initConnection(DataMap config) {
        try {
            if (conn == null) {
                if (postgresConfig == null) {
                    postgresConfig = PostgresConfig.load(config);
                }
                String dbUrl = postgresConfig.getDatabaseUrl();
                Class.forName(postgresConfig.getJdbcDriver());
                conn = DriverManager.getConnection(dbUrl, postgresConfig.getUser(), postgresConfig.getPassword());
            }
            if (stmt == null) {
                stmt = conn.createStatement();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Connection Failed!");
        }

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());

        List<TapTable> tapTableList = new LinkedList<>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), postgresConfig.getSchema(), null, new String[]{"TABLE"});
        while (tableResult.next()) {
            String tableName = tableResult.getString("TABLE_NAME");
            TapTable table = table(tableName);
            ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), postgresConfig.getSchema(), tableName, null);
            while (columnsResult.next()) {
                TapField tapField = new PostgresColumn(columnsResult).getTapField();
                table.add(tapField);
            }
            tapTableList.add(table);
        }
        consumer.accept(tapTableList);
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        initConnection(connectionContext.getConnectionConfig());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {

        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportAlterTable(this::alterTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null)
                return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
    }

    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, Consumer<List<FilterResult>> listConsumer) {
        initConnection(connectorContext.getConnectionConfig());
        TapTable tapTable = connectorContext.getTable();
        Set<String> columnNames = connectorContext.getTable().getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM " + tapTable.getName() + " WHERE " + SqlBuilder.buildKeyAndValue(filter.getMatch(), "AND");
            FilterResult filterResult = new FilterResult();
            try {
                DataMap resultMap = new DataMap();
                ResultSet resultSet = stmt.executeQuery(sql);
                if (resultSet.next()) {
                    for (String columnName : columnNames) {
                        resultMap.put(columnName, resultSet.getObject(columnName));
                    }
                    filterResult.setResult(resultMap);
                    break;
                }
            } catch (SQLException e) {
//                e.printStackTrace();
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);

    }


    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String sql = "CREATE TABLE " + tapTable.getName() +
                "(" + SqlBuilder.buildColumnDefinition(tapTable) + "," +
                " UNIQUE (" + StringKit.combineStringWithComma(primaryKeys) + " ) )";

        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getName() + " Failed! " + e.getMessage());
        }
    }

//FIXME DOIRS异步执行alter命令，无回调接口，没次对同一个table同时执行一个alter命令；不能保证某个时刻是否存在alter命令正在执行

//    private void alterTable(TapConnectorContext tapConnectorContext, TapAlterTableEvent tapAlterTableEvent)
//        // TODO 需要实现修改表的功能， 不过测试只能先从源端模拟一个修改表事件
//        initConnection(tapConnectorContext.getConnectionConfig());
//        TapTable tapTable = tapConnectorContext.getTable();
//        Set<String> fieldNames = tapTable.getNameFieldMap().keySet();
//        try {
//            for (TapField insertField : tapAlterTableEvent.getInsertFields()) {
//                if (insertField.getOriginType() == null || insertField.getDefaultValue() == null) continue;
//                String sql = "ALTER TABLE " + tapTable.getName() +
//                        " ADD COLUMN " + insertField.getName() + ' ' + insertField.getOriginType() +
//                        " DEFAULT '" + insertField.getDefaultValue() + "'";
//                stmt.execute(sql);
//            }
//            for (String deletedFieldName : tapAlterTableEvent.getDeletedFields()) {
//                if (!fieldNames.contains(deletedFieldName)) continue;
//                String sql = "ALTER TABLE " + tapTable.getName() +
//                        " DROP COLUMN " + deletedFieldName;
//                stmt.execute(sql);
//            }
//            // TODO Doris在文档中没有看到修改列名的相关操作
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw new RuntimeException("ALTER Table " + tapTable.getName() + " Failed! \n ");
//        }
//
//        PDKLogger.info(TAG, "alterTable");
//    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapClearTableEvent.getTable();
        try {
            ResultSet table = conn.getMetaData().getTables(null, postgresConfig.getDatabase(), tapTable.getName(), new String[]{TABLE_COLUMN_NAME});
            if (table.first()) {
                String sql = "TRUNCATE TABLE " + tapTable.getName();
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException("TRUNCATE Table " + tapTable.getName() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTable();
        try {
            ResultSet table = conn.getMetaData().getTables(null, postgresConfig.getDatabase(), tapTable.getName(), new String[]{});
            if (table.first()) {
                String sql = "DROP TABLE " + tapTable.getName();
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapTable.getName() + " Failed! \n ");
        }
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        initConnection(connectorContext.getConnectionConfig());
        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count


        TapTable tapTable = connectorContext.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        PreparedStatement preparedStatement = conn.prepareStatement(SqlBuilder.buildPrepareInsertSQL(tapTable));
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            ResultSet table = conn.getMetaData().getTables(postgresConfig.getDatabase(), postgresConfig.getSchema(), tapTable.getName().toLowerCase(), new String[]{TABLE_COLUMN_NAME});
            if (!table.first()) throw new RuntimeException("Table " + tapTable.getName() + " not exist!");
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                SqlBuilder.addBatchInsertRecord(tapTable, after, preparedStatement);
                inserted.incrementAndGet();
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                executeBatchInsert(preparedStatement);
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> before = updateRecordEvent.getBefore();
                for (Map.Entry<String, Object> entry : before.entrySet()) {
                    after.remove(entry.getKey(), entry.getValue());
                }

                String sql = "UPDATE " + tapTable.getName() +
                        " SET " + SqlBuilder.buildKeyAndValue(after, ",") +
                        " WHERE " + SqlBuilder.buildKeyAndValue(before, "AND");
                stmt.execute(sql);
                updated.incrementAndGet();
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                executeBatchInsert(preparedStatement);
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();
                String sql = "DELETE FROM " + tapTable.getName() + " WHERE " + SqlBuilder.buildKeyAndValue(before, "AND");
                stmt.execute(sql);
                deleted.incrementAndGet();
            }
        }
        executeBatchInsert(preparedStatement);
        //Need to tell flow engine the write result
        preparedStatement.close();
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    private void executeBatchInsert(PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
                stmt = null;
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
