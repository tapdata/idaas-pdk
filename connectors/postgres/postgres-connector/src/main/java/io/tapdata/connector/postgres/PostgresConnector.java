package io.tapdata.connector.postgres;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.bean.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.kit.SmartKit;
import io.tapdata.connector.postgres.kit.SqlBuilder;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_postgres.json")
public class PostgresConnector extends ConnectorBase {

    private PostgresConfig postgresConfig;
    private Connection conn;
    private Statement stmt;
    private PostgresCdcRunner cdcRunner;
    private static final String TABLE_COLUMN_NAME = "TABLE";
    private static final int BATCH_READ_SIZE = 5000;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    @Override
    public void onStart(TapConnectionContext connectorContext) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());
        List<TapTable> tapTableList = new LinkedList<>();
        AtomicInteger tableCount = new AtomicInteger(0);
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        //get table info
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), postgresConfig.getSchema(), null, new String[]{TABLE_COLUMN_NAME});
        while (tableResult.next()) {
            String tableName = tableResult.getString("TABLE_NAME");
            //1、filter by tableList
            if (tables != null && tables.stream().noneMatch(tableName::equals)) {
                continue;
            }
            //2、table name
            TapTable table = table(tableName);
            //3、table columns info
            ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), postgresConfig.getSchema(), tableName, null);
            while (columnsResult.next()) {
                TapField tapField = new PostgresColumn(columnsResult).getTapField();
                table.add(tapField);
            }
            //4、primary key
            ResultSet primaryKeyResult = databaseMetaData.getPrimaryKeys(conn.getCatalog(), postgresConfig.getSchema(), tableName);
            table.setDefaultPrimaryKeys(getAllFromResultSet(primaryKeyResult).stream().sorted(Comparator.comparing(v -> (int) v.get("key_seq"))).map(v -> (String) v.get("column_name")).collect(Collectors.toList()));
            //5、table index
            ResultSet indexResult = databaseMetaData.getIndexInfo(conn.getCatalog(), postgresConfig.getSchema(), tableName, false, false);
            Map<String, List<DataMap>> indexMap = getAllFromResultSet(indexResult).stream().sorted(Comparator.comparing(v -> (int) v.get("ORDINAL_POSITION"))).collect(Collectors.groupingBy(v -> (String) v.get("INDEX_NAME"), LinkedHashMap::new, Collectors.toList()));
            indexMap.forEach((key, value) -> {
                TapIndex index = new TapIndex();
                index.setName(key);
                index.setUnique(value.stream().anyMatch(v -> !(boolean) v.get("NON_UNIQUE")));
                index.setIndexFields(value.stream().map(v -> {
                    TapIndexField field = new TapIndexField();
                    field.setName((String) v.get("COLUMN_NAME"));
                    field.setFieldAsc("A".equals(v.get("ASC_OR_DESC")));
                    return field;
                }).collect(Collectors.toList()));
                table.add(index);
            });
            tapTableList.add(table);
            if (tableCount.incrementAndGet() == tableSize) {
                consumer.accept(tapTableList);
                tableCount = new AtomicInteger(0);
                tapTableList.clear();
            }
        }
        consumer.accept(tapTableList);
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        PostgresTest postgresTest = new PostgresTest(connectionContext);
        TestItem testHostPort = postgresTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return;
        }
        TestItem testConnect = postgresTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return;
        }
        consumer.accept(postgresTest.testPrivilege());
        consumer.accept(postgresTest.testReplication());
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), postgresConfig.getSchema(), null, new String[]{"TABLE"});
        tableResult.last();
        return tableResult.getRow();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportAlterTable(this::alterTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportStreamOffset(this::streamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
//        codecRegistry.registerFromTapValue(TapTimeValue.class, "text", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
//        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
//        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapTimeValue.class, "text", tapTimeValue -> tapTimeValue.getValue().toString());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, "text", tapDateValue -> tapDateValue.getValue().toString());
    }

    @Override
    public void onDestroy() {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
                stmt = null;
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
            if (cdcRunner != null) {
                cdcRunner.closeCdcRunner();
                cdcRunner = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

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

    //one filter can only match one record
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        initConnection(connectorContext.getConnectionConfig());
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + tapTable.getId() + "\" WHERE " + SqlBuilder.buildKeyAndValue(filter.getMatch(), "AND", "=");
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
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        FilterResults filterResults = new FilterResults();
        String sql = "SELECT * FROM \"" + table.getId() + "\" " + SqlBuilder.buildSqlByAdvanceFilter(filter);
        ResultSet resultSet = stmt.executeQuery(sql);
        while (resultSet.next()) {
            filterResults.add(getRowFromResultSet(resultSet, getColumnsFromResultSet(resultSet)));
            if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                consumer.accept(filterResults);
            }
        }
        if (SmartKit.isNotEmpty(filterResults.getResults())) {
            consumer.accept(filterResults);
        }
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTableMap().get(tapCreateTableEvent.getTableId());
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS \"" + tapTable.getId() + "\"(" + SqlBuilder.buildColumnDefinition(tapTable);
        if (SmartKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + SmartKit.combineString(primaryKeys, "\",\"") + "\" )";
        }
        sql += ")";
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            //comment on table and column
            if (null != tapTable.getComment()) {
                stmt.execute("COMMENT ON TABLE \"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (null != fieldComment) {
                    stmt.execute("COMMENT ON COLUMN \"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            // TODO: 2022/5/16 how to deal with table index
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
    }

//    private void alterTable(TapConnectorContext tapConnectorContext, TapAlterTableEvent tapAlterTableEvent)
//        initConnection(tapConnectorContext.getConnectionConfig());
//        TapTable tapTable = tapConnectorContext.getTable();
//        Set<String> fieldNames = tapTable.getNameFieldMap().keySet();
//        try {
//            for (TapField insertField : tapAlterTableEvent.getInsertFields()) {
//                if (insertField.getOriginType() == null || insertField.getDefaultValue() == null) continue;
//                String sql = "ALTER TABLE " + tapTable.getId() +
//                        " ADD COLUMN " + insertField.getName() + ' ' + insertField.getOriginType() +
//                        " DEFAULT '" + insertField.getDefaultValue() + "'";
//                stmt.execute(sql);
//            }
//            for (String deletedFieldName : tapAlterTableEvent.getDeletedFields()) {
//                if (!fieldNames.contains(deletedFieldName)) continue;
//                String sql = "ALTER TABLE " + tapTable.getId() +
//                        " DROP COLUMN " + deletedFieldName;
//                stmt.execute(sql);
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw new RuntimeException("ALTER Table " + tapTable.getId() + " Failed! \n ");
//        }
//
//        PDKLogger.info(TAG, "alterTable");
//    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTableMap().get(tapClearTableEvent.getTableId());
        try {
            ResultSet table = conn.getMetaData().getTables(conn.getCatalog(), postgresConfig.getSchema(), tapTable.getId(), new String[]{TABLE_COLUMN_NAME});
            if (table.first()) {
                String sql = "TRUNCATE TABLE \"" + tapTable.getId() + "\"";
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException("TRUNCATE Table " + tapTable.getId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTableMap().get(tapDropTableEvent.getTableId());
        try {
            ResultSet table = conn.getMetaData().getTables(conn.getCatalog(), postgresConfig.getSchema(), tapTable.getId(), new String[]{TABLE_COLUMN_NAME});
            if (table.first()) {
                String sql = "DROP TABLE \"" + tapTable.getId() + "\""; // DROP TABLE IF EXISTS
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapTable.getId() + " Failed! \n ");
        }
    }

    // TODO: 2022/5/13 the same type of event must be dealt with to make this method faster
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        initConnection(connectorContext.getConnectionConfig());
        AtomicLong inserted = new AtomicLong(0); //number of inserted
        AtomicLong updated = new AtomicLong(0); //number of updated
        AtomicLong deleted = new AtomicLong(0); //number of deleted
        WriteListResult<TapRecordEvent> listResult = writeListResult(); //result of these events
        List<TapRecordEvent> batchInsertCache = list(); //records in batch cache

        PreparedStatement preparedStatement = conn.prepareStatement(SqlBuilder.buildPrepareInsertSQL(tapTable));
        ResultSet table = conn.getMetaData().getTables(postgresConfig.getDatabase(), postgresConfig.getSchema(), tapTable.getId(), new String[]{TABLE_COLUMN_NAME});
        if (!table.first()) {
            throw new RuntimeException("Table " + tapTable.getId() + " not exist!");
        }
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                SqlBuilder.addBatchInsertRecord(tapTable, after, preparedStatement);
                batchInsertCache.add(recordEvent);
                if (batchInsertCache.size() >= 1000) {
                    inserted.addAndGet(executeBatchInsert(preparedStatement, batchInsertCache, listResult));
                }
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                inserted.addAndGet(executeBatchInsert(preparedStatement, batchInsertCache, listResult));
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> before = updateRecordEvent.getBefore();
                for (Map.Entry<String, Object> entry : before.entrySet()) {
                    after.remove(entry.getKey(), entry.getValue());
                }
                String sql = "UPDATE \"" + tapTable.getId() + "\" SET " + SqlBuilder.buildKeyAndValue(after, ",", "=") + " WHERE " + SqlBuilder.buildKeyAndValue(before, "AND", "=");
                try {
                    stmt.execute(sql);
                    updated.incrementAndGet();
                } catch (SQLException e) {
                    listResult.addError(recordEvent, e);
                    e.printStackTrace();
                }
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                inserted.addAndGet(executeBatchInsert(preparedStatement, batchInsertCache, listResult));
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();
                String sql = "DELETE FROM \"" + tapTable.getId() + "\" WHERE " + SqlBuilder.buildKeyAndValue(before, "AND", "=");
                try {
                    stmt.execute(sql);
                    deleted.incrementAndGet();
                } catch (SQLException e) {
                    listResult.addError(recordEvent, e);
                    e.printStackTrace();
                }
            }
        }
        inserted.addAndGet(executeBatchInsert(preparedStatement, batchInsertCache, listResult));
        preparedStatement.close();
        writeListResultConsumer.accept(listResult.insertedCount(inserted.get()).modifiedCount(updated.get()).removedCount(deleted.get()));
    }

    private long executeBatchInsert(PreparedStatement preparedStatement, List<TapRecordEvent> batchInsertCache, WriteListResult<TapRecordEvent> listResult) {
        long succeed = batchInsertCache.size();
        try {
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                batchInsertCache.clear();
            }
        } catch (SQLException e) {
            Map<TapRecordEvent, Throwable> map = batchInsertCache.stream().collect(Collectors.toMap(Function.identity(), (v) -> e));
            listResult.addErrors(map);
            succeed = 0;
            e.printStackTrace();
        }
        return succeed;
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws SQLException {
        initConnection(tapConnectorContext.getConnectionConfig());
        String sql = "SELECT COUNT(1) FROM \"" + tapTable.getId() + "\"";
        ResultSet resultSet = stmt.executeQuery(sql);
        if (resultSet.next()) {
            return resultSet.getLong(1);
        }
        return 0;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws SQLException {
        initConnection(tapConnectorContext.getConnectionConfig());
        List<TapEvent> tapEvents = list();
        PostgresOffset postgresOffset;
        //beginning
        if (null == offsetState) {
            postgresOffset = new PostgresOffset(getOrderByUniqueKey(tapTable), 0L);
        }
        //with offset
        else {
            postgresOffset = (PostgresOffset) offsetState;
        }
        String sql = "SELECT * FROM \"" + tapTable.getId() + "\"" + postgresOffset.getSortString() + " OFFSET " + postgresOffset.getOffsetValue() + " LIMIT " + BATCH_READ_SIZE;
        ResultSet resultSet = stmt.executeQuery(sql);
        //get all column names
        List<String> columnNames = getColumnsFromResultSet(resultSet);
        while (resultSet.next()) {
            tapEvents.add(insertRecordEvent(getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
            if (tapEvents.size() == eventBatchSize) {
                postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + eventBatchSize);
                eventsOffsetConsumer.accept(tapEvents, postgresOffset);
                tapEvents = list();
            }
        }
        //last events those less than eventBatchSize
        if (SmartKit.isNotEmpty(tapEvents)) {
            postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + tapEvents.size());
            eventsOffsetConsumer.accept(tapEvents, postgresOffset);
        }
    }

    private String getOrderByUniqueKey(TapTable tapTable) {
        StringBuilder orderBy = new StringBuilder();
        List<TapIndex> indexList = tapTable.getIndexList();
        if (SmartKit.isNotEmpty(indexList) && indexList.stream().anyMatch(TapIndex::isUnique)) {
            orderBy.append(" ORDER BY ");
            TapIndex uniqueIndex = indexList.stream().filter(TapIndex::isUnique).findFirst().orElseGet(TapIndex::new);
            for (int i = 0; i < uniqueIndex.getIndexFields().size(); i++) {
                String ascOrDesc = uniqueIndex.getIndexFields().get(i).getFieldAsc() ? "ASC" : "DESC";
                orderBy.append('\"').append(uniqueIndex.getIndexFields().get(i).getName()).append("\" ").append(ascOrDesc).append(',');
            }
        }
        // TODO: 2022/5/7 how to deal with which without unique key
        orderBy.delete(orderBy.length() - 1, orderBy.length());
        return orderBy.toString();
    }

    private List<DataMap> getAllFromResultSet(ResultSet resultSet) throws SQLException {
        List<DataMap> list = new LinkedList<>();
        if (resultSet != null) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                DataMap map = DataMap.create();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                }
                list.add(map);
            }
        }
        return list;
    }

    private DataMap getRowFromResultSet(ResultSet resultSet, List<String> columnNames) throws SQLException {
        DataMap map = DataMap.create();
        if (resultSet != null) {
            for (int i = 0; i < columnNames.size(); i++) {
                map.put(columnNames.get(i), resultSet.getObject(i + 1));
            }
        }
        return map;
    }

    private List<String> getColumnsFromResultSet(ResultSet resultSet) throws SQLException {
        //get all column names
        List<String> columnNames = list();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            columnNames.add(resultSetMetaData.getColumnName(i));
        }
        return columnNames;
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        initConnection(nodeContext.getConnectionConfig());
        if (cdcRunner == null) {
            List<TapTable> tapTableList = null;
            if (SmartKit.isNotEmpty(tableList)) {
                tapTableList = tableList.stream().map(v -> nodeContext.getTableMap().get(v)).collect(Collectors.toList());
            }
            cdcRunner = new PostgresCdcRunner(postgresConfig, tapTableList).registerConsumer(offsetState, recordSize, consumer);
//            DebeziumCdcPool.addRunner(cdcRunner.getRunnerName(), cdcRunner);
            cdcRunner.startCdcRunner();
        }
        while (!cdcRunner.isRunning()) {
            sleep(100L);
        }
        sleep(5000);
        consumer.streamReadStarted(true);
    }

    // TODO: 2022/5/14 implement with offset
    private Object streamOffset(TapConnectorContext connectorContext, List<String> tableList, Long offsetStartTime) throws Throwable {
        return null;
    }

}
