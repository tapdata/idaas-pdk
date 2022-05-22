package io.tapdata.connector.postgres;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.kit.DbKit;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.connector.postgres.kit.StringKit;
import io.tapdata.connector.postgres.storage.PostgresOffsetStorage;
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
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
    private PostgresJdbcContext postgresJdbcContext;
    private PostgresCdcRunner cdcRunner;
    private static final int BATCH_READ_SIZE = 5000;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        List<TapTable> tapTableList = new LinkedList<>();
        //get table info
        List<String> tableList = postgresJdbcContext.queryAllTables(null);
        List<DataMap> columnList = postgresJdbcContext.queryAllColumns(null);
        List<DataMap> indexList = postgresJdbcContext.queryAllIndexes(null);
        //1、filter by tableList
        if (EmptyKit.isNotEmpty(tables)) {
            tableList.removeIf(t -> !tables.contains(t));
            columnList.removeIf(c -> !tables.contains(c.getString("table_name")));
            indexList.removeIf(i -> !tables.contains(i.getString("table_name")));
        }
        tableList.forEach(table -> {
            //2、table name
            TapTable tapTable = table(table);
            //3、primary key and table index
            List<String> primaryKey = TapSimplify.list();
            List<TapIndex> tapIndexList = TapSimplify.list();
            Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("table_name")))
                    .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
            indexMap.forEach((key, value) -> {
                if (value.stream().anyMatch(v -> (boolean) v.get("is_primary"))) {
                    primaryKey.addAll(value.stream().map(v -> v.getString("column_name")).collect(Collectors.toList()));
                }
                TapIndex index = new TapIndex();
                List<TapIndexField> fieldList = TapSimplify.list();
                value.forEach(v -> {
                    TapIndexField field = new TapIndexField();
                    field.setFieldAsc("A".equals(v.getString("asc_or_desc")));
                    field.setName(v.getString("column_name"));
                    fieldList.add(field);
                });
                index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
                index.setIndexFields(fieldList);
                tapIndexList.add(index);
            });
            //4、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                    .forEach(col -> {
                        TapField tapField = new PostgresColumn(col).getTapField();
                        tapField.setPos(keyPos.incrementAndGet());
                        tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                        tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                        tapTable.add(tapField);
                    });
            tapTable.setIndexList(tapIndexList);
            tapTableList.add(tapTable);
            if (tapTableList.size() == tableSize) {
                consumer.accept(tapTableList);
                tapTableList.clear();
            }
        });
        consumer.accept(tapTableList);
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        PostgresTest postgresTest = new PostgresTest(postgresConfig);
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
        postgresTest.close();
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return postgresJdbcContext.queryAllTables(null).size();
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
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportStreamOffset(this::streamOffset);
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
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
//        codecRegistry.registerFromTapValue(TapTimeValue.class, "text", tapTimeValue -> tapTimeValue.getValue().toString());
//        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
//        codecRegistry.registerFromTapValue(TapDateValue.class, "text", tapDateValue -> tapDateValue.getValue().toString());
    }

    @Override
    public void onDestroy() throws IOException {
        if (EmptyKit.isNotNull(postgresJdbcContext)) {
            postgresJdbcContext.close();
        }
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner(true);
            cdcRunner = null;
        }
        //stateMap will be cleared by engine
    }

    @Override
    public void onPause() throws Throwable {
        if (EmptyKit.isNotNull(postgresJdbcContext)) {
            postgresJdbcContext.close();
        }
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner(false);
            cdcRunner = null;
        }
    }

    private void initConnection(TapConnectionContext connectorContext) {
        postgresConfig = PostgresConfig.load(connectorContext.getConnectionConfig());
        if (EmptyKit.isNull(postgresJdbcContext)) {
            postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        }
    }

    //one filter can only match one record
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + tapTable.getId() + "\" WHERE " + PostgresSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                postgresJdbcContext.query(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        FilterResults filterResults = new FilterResults();
        String sql = "SELECT * FROM \"" + table.getId() + "\" " + PostgresSqlMaker.buildSqlByAdvanceFilter(filter);
        postgresJdbcContext.query(sql, resultSet -> {
            while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                }
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS \"" + tapTable.getId() + "\"(" + PostgresSqlMaker.buildColumnDefinition(tapTable);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + StringKit.combineString(primaryKeys, "\",\"") + "\" )";
        }
        sql += ")";
        try {
            postgresJdbcContext.execute(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                postgresJdbcContext.execute("COMMENT ON TABLE \"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    postgresJdbcContext.execute("COMMENT ON COLUMN \"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            // TODO: 2022/5/16 how to deal with table index
        } catch (Throwable e) {
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
        try {
            if (postgresJdbcContext.queryAllTables(tapClearTableEvent.getTableId()).size() == 1) {
                postgresJdbcContext.execute("TRUNCATE TABLE \"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (postgresJdbcContext.queryAllTables(tapDropTableEvent.getTableId()).size() == 1) {
                postgresJdbcContext.execute("DROP TABLE IF EXISTS \"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    // TODO: 2022/5/13 the same type of event must be dealt with to make this method faster
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        AtomicLong inserted = new AtomicLong(0); //number of insertedtapTable = {TapTable@10447} "TapTable id BatchReadTest_postgresToTddTarget_ISfJVJ name BatchReadTest_postgresToTddTarget_ISfJVJ storageEngine null charset null number of fields 21"
        AtomicLong updated = new AtomicLong(0); //number of updated
        AtomicLong deleted = new AtomicLong(0); //number of deleted
        WriteListResult<TapRecordEvent> listResult = writeListResult(); //result of these events
        List<TapRecordEvent> batchInsertCache = list(); //records in batch cache

        PreparedStatement preparedStatement = postgresJdbcContext.getConnection().prepareStatement(PostgresSqlMaker.buildPrepareInsertSQL(tapTable));
        Statement stmt = postgresJdbcContext.getConnection().createStatement();
        if (postgresJdbcContext.queryAllTables(tapTable.getId()).size() < 1) {
            throw new RuntimeException("Table " + tapTable.getId() + " not exist!");
        }
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                PostgresSqlMaker.addBatchInsertRecord(tapTable, after, preparedStatement);
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
                String sql = "UPDATE \"" + tapTable.getId() + "\" SET " + PostgresSqlMaker.buildKeyAndValue(after, ",", "=") + " WHERE " + PostgresSqlMaker.buildKeyAndValue(before, "AND", "=");
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
                String sql = "DELETE FROM \"" + tapTable.getId() + "\" WHERE " + PostgresSqlMaker.buildKeyAndValue(before, "AND", "=");
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

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + tapTable.getId() + "\"";
        postgresJdbcContext.query(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
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
        postgresJdbcContext.query(sql, resultSet -> {
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + eventBatchSize);
                    eventsOffsetConsumer.accept(tapEvents, postgresOffset);
                    tapEvents.clear();
                }
                resultSet.next();
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + tapEvents.size());
                eventsOffsetConsumer.accept(tapEvents, postgresOffset);
            }
        });

    }

    private String getOrderByUniqueKey(TapTable tapTable) {
        StringBuilder orderBy = new StringBuilder();
        List<TapIndex> indexList = tapTable.getIndexList();
        if (EmptyKit.isNotEmpty(indexList) && indexList.stream().anyMatch(TapIndex::isUnique)) {
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

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        if (cdcRunner == null) {
            cdcRunner = new PostgresCdcRunner()
                    .use(postgresConfig)
                    .watch(tableList)
                    .offset(offsetState)
                    .registerConsumer(consumer, recordSize);
            if (EmptyKit.isNotNull(nodeContext.getStateMap().get("manyOffsetMap"))) {
                PostgresOffsetStorage.manyOffsetMap = (Map) nodeContext.getStateMap().get("manyOffsetMap");
            }
            cdcRunner.startCdcRunner();
        }
    }

    // TODO: 2022/5/14 implement with offset
    private void streamOffset(TapConnectorContext connectorContext, List<String> tableList, Long offsetStartTime, BiConsumer<Object, Long> offsetOffsetTimeConsumer) {
        //engine get last offset
        if (EmptyKit.isNull(offsetStartTime)) {
            PostgresOffset postgresOffset = PostgresOffsetStorage.postgresOffsetMap.get(cdcRunner.getRunnerName());
            if (EmptyKit.isNull(postgresOffset)) {
                offsetOffsetTimeConsumer.accept(null, null);
            } else {
                offsetOffsetTimeConsumer.accept(postgresOffset, postgresOffset.getStreamOffsetTime());
            }
        }
        //engine get last offset which is before offsetStartTime
        else {
            List<PostgresOffset> list = PostgresOffsetStorage.manyOffsetMap.get(cdcRunner.getRunnerName());
            list.removeIf(offset -> offset.getStreamOffsetTime() > offsetStartTime);
            PostgresOffset postgresOffset = list.stream().max(Comparator.comparing(PostgresOffset::getStreamOffsetTime)).orElseGet(PostgresOffset::new);
            if (EmptyKit.isNull(postgresOffset.getStreamOffsetKey())) {
                offsetOffsetTimeConsumer.accept(null, null);
            } else {
                offsetOffsetTimeConsumer.accept(postgresOffset, postgresOffset.getStreamOffsetTime());
            }
        }
        connectorContext.getStateMap().put("manyOffsetMap", PostgresOffsetStorage.manyOffsetMap);
    }

}
