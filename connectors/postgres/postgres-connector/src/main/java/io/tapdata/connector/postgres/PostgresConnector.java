package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
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
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    private String postgresVersion;
    private static final int BATCH_READ_SIZE = 5000;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = postgresJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("table_name")).collect(Collectors.toList());
            List<DataMap> columnList = postgresJdbcContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = postgresJdbcContext.queryAllIndexes(subTableNames);
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("table_name");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("comment"));
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
                    index.setName(key);
                    List<TapIndexField> fieldList = TapSimplify.list();
                    value.forEach(v -> {
                        TapIndexField field = new TapIndexField();
                        field.setFieldAsc("A".equals(v.getString("asc_or_desc")));
                        field.setName(v.getString("column_name"));
                        fieldList.add(field);
                    });
                    index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
                    index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
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
            });
            consumer.accept(tapTableList);
        });
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
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
        initConnection(connectionContext);
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
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
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
    public void onDestroy(TapConnectionContext connectionContext) throws IOException, SQLException {
        if (EmptyKit.isNotNull(postgresJdbcContext)) {
            postgresJdbcContext.finish();
        }
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner(true);
            cdcRunner = null;
        }
        //stateMap will be cleared by engine
    }

    @Override
    public void onPause(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(postgresJdbcContext)) {
            postgresJdbcContext.finish();
        }
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner(false);
            cdcRunner = null;
        }
    }

    private void initConnection(TapConnectionContext connectorContext) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectorContext.getConnectionConfig());
        if (EmptyKit.isNull(postgresJdbcContext)) {
            postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class);
        }
        postgresVersion = postgresJdbcContext.queryVersion();
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
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            if (EmptyKit.isNotEmpty(tapTable.getIndexList())) {
                if (postgresVersion.compareTo("PostgreSQL 9.5") > 0) {
                    tapTable.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX IF NOT EXISTS \"" + i.getName() + "\" " + "ON \"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                } else {
                    List<String> existsIndexes = TapSimplify.list();
                    postgresJdbcContext.query("SELECT relname FROM pg_class WHERE relname IN (" +
                                    tapTable.getIndexList().stream().map(i -> "'" + i.getName() + "'").collect(Collectors.joining(",")) + ") AND relkind = 'i'",
                            resultSet -> existsIndexes.addAll(DbKit.getDataFromResultSet(resultSet).stream().map(v -> v.getString("relname")).collect(Collectors.toList())));
                    tapTable.getIndexList().stream().filter(i -> !i.isPrimary() && !existsIndexes.contains(i.getName())).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX \"" + i.getName() + "\" " + "ON \"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                }
            }
            postgresJdbcContext.batchExecute(sqls);
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
            if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                postgresJdbcContext.execute("TRUNCATE TABLE \"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                postgresJdbcContext.execute("DROP TABLE IF EXISTS \"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() < 1) {
            throw new RuntimeException("Table " + tapTable.getId() + " not exist!");
        }
        Connection connection = postgresJdbcContext.getConnection();
        //three types of record
        PostgresWriteRecorder insertRecorder = new PostgresWriteRecorder(connection, tapTable);
        PostgresWriteRecorder updateRecorder = new PostgresWriteRecorder(connection, tapTable);
        PostgresWriteRecorder deleteRecorder = new PostgresWriteRecorder(connection, tapTable);

        if (postgresVersion.compareTo("PostgreSQL 9.5") <= 0) {
            insertRecorder.setPostgresVersion(postgresVersion);
            updateRecorder.setPostgresVersion(postgresVersion);
            deleteRecorder.setPostgresVersion(postgresVersion);
        }
        //result of these events
        WriteListResult<TapRecordEvent> listResult = writeListResult();
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                updateRecorder.executeBatch(listResult);
                deleteRecorder.executeBatch(listResult);
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                insertRecorder.addInsertBatch(insertRecordEvent.getAfter());
                insertRecorder.addAndCheckCommit(recordEvent, listResult);
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                insertRecorder.executeBatch(listResult);
                deleteRecorder.executeBatch(listResult);
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                updateRecorder.addUpdateBatch(updateRecordEvent.getBefore(), updateRecordEvent.getAfter());
                updateRecorder.addAndCheckCommit(recordEvent, listResult);
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                insertRecorder.executeBatch(listResult);
                updateRecorder.executeBatch(listResult);
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                deleteRecorder.addDeleteBatch(deleteRecordEvent.getBefore());
                deleteRecorder.addAndCheckCommit(recordEvent, listResult);
            }
        }
        insertRecorder.executeBatch(listResult);
        updateRecorder.executeBatch(listResult);
        deleteRecorder.executeBatch(listResult);
        connection.commit();
        insertRecorder.releaseResource();
        updateRecorder.releaseResource();
        deleteRecorder.releaseResource();
        connection.close();
        writeListResultConsumer.accept(listResult
                .insertedCount(insertRecorder.getAtomicLong().get())
                .modifiedCount(updateRecorder.getAtomicLong().get())
                .removedCount(deleteRecorder.getAtomicLong().get()));
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
            }
            eventsOffsetConsumer.accept(tapEvents, postgresOffset);
        });

    }

    private String getOrderByUniqueKey(TapTable tapTable) {
        StringBuilder orderBy = new StringBuilder();
        orderBy.append(" ORDER BY ");
        List<TapIndex> indexList = tapTable.getIndexList();
        //has no indexes, need each field
        if (EmptyKit.isEmpty(indexList)) {
            orderBy.append(tapTable.getNameFieldMap().keySet().stream().map(field -> "\"" + field + "\"")
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        //has indexes but no unique
        else if (indexList.stream().noneMatch(TapIndex::isUnique)) {
            TapIndex index = indexList.stream().findFirst().orElseGet(TapIndex::new);
            orderBy.append(index.getIndexFields().stream().map(field -> "\"" + field.getName() + "\" " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            List<String> indexFields = index.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList());
            if (tapTable.getNameFieldMap().size() > indexFields.size()) {
                orderBy.append(',');
                orderBy.append(tapTable.getNameFieldMap().keySet().stream().filter(key -> !indexFields.contains(key)).map(field -> "\"" + field + "\"")
                        .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
            }
        }
        //has unique indexes
        else {
            TapIndex uniqueIndex = indexList.stream().filter(TapIndex::isUnique).findFirst().orElseGet(TapIndex::new);
            orderBy.append(uniqueIndex.getIndexFields().stream().map(field -> "\"" + field.getName() + "\" " + (field.getFieldAsc() ? "ASC" : "DESC"))
                    .reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new));
        }
        return orderBy.toString();
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        if (cdcRunner == null) {
            Object slotName = nodeContext.getStateMap().get("tapdata_pg_slot");
            cdcRunner = new PostgresCdcRunner().use(postgresConfig);
            if (EmptyKit.isNull(slotName)) {
                cdcRunner.watch(tableList).offset(offsetState).registerConsumer(consumer, recordSize);
                nodeContext.getStateMap().put("tapdata_pg_slot", cdcRunner.getRunnerName());
            } else {
                cdcRunner.useSlot(slotName.toString()).watch(tableList).offset(offsetState).registerConsumer(consumer, recordSize);
            }
//            if (EmptyKit.isNotNull(nodeContext.getStateMap().get("manyOffsetMap"))) {
//                PostgresOffsetStorage.manyOffsetMap = (Map) nodeContext.getStateMap().get("manyOffsetMap");
//            }
            cdcRunner.startCdcRunner();
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        //engine get last offset
//        if (EmptyKit.isNull(offsetStartTime)) {
//            return null;
//        }
//        //engine get last offset which is before offsetStartTime
//        else {
//            List<PostgresOffset> list = PostgresOffsetStorage.manyOffsetMap.get(cdcRunner.getRunnerName());
//            list.removeIf(offset -> offset.getStreamOffsetTime() > offsetStartTime);
//            PostgresOffset postgresOffset = list.stream().max(Comparator.comparing(PostgresOffset::getStreamOffsetTime)).orElseGet(PostgresOffset::new);
//            if (EmptyKit.isNull(postgresOffset.getStreamOffsetKey())) {
//                return null;
//            } else {
//                return postgresOffset;
//            }
//        }
        return new PostgresOffset();
//        connectorContext.getStateMap().put("manyOffsetMap", PostgresOffsetStorage.manyOffsetMap);
    }

}
