package io.tapdata.connector.oracle;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.oracle.bean.OracleColumn;
import io.tapdata.connector.oracle.cdc.OracleCdcRunner;
import io.tapdata.connector.oracle.cdc.offset.OracleOffset;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
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

import java.math.BigDecimal;
import oracle.sql.BLOB;
import oracle.sql.TIMESTAMP;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_oracle.json")
public class OracleConnector extends ConnectorBase {

    private OracleConfig oracleConfig;
    private OracleJdbcContext oracleJdbcContext;
    private OracleCdcRunner cdcRunner;
    private String oracleVersion;
    private static final int BATCH_READ_SIZE = 5000;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    //initialize jdbc context, version
    private void initConnection(TapConnectionContext connectorContext) {
        oracleConfig = new OracleConfig().load(connectorContext.getConnectionConfig());
        if (EmptyKit.isNull(oracleJdbcContext) || oracleJdbcContext.isFinish()) {
            oracleJdbcContext = (OracleJdbcContext) DataSourcePool.getJdbcContext(oracleConfig, OracleJdbcContext.class, connectorContext.getId());
        }
        oracleVersion = oracleJdbcContext.queryVersion();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(oracleJdbcContext)) {
            oracleJdbcContext.finish(connectionContext.getId());
        }
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner();
            cdcRunner = null;
        }
    }

    private void onDestroy(TapConnectionContext connectionContext) {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //need to clear resource outer
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        //target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportCreateIndex(this::createIndex);
//        //source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
//        //query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "CLOB", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "CLOB", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "CLOB", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "INTEGER", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return tapValue.getValue() ? 1 : 0;
            return 0;
        });

        codecRegistry.registerToTapValue(TIMESTAMP.class, (value, tapType) -> {
            try {
                return new TapDateTimeValue(new DateTime(((TIMESTAMP)value).timestampValue()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(BLOB.class, (value, tapType) -> new TapBinaryValue(DbKit.BlobToBytes((BLOB) value)));
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, "CHAR(8)", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        new OracleRecordWriter(oracleJdbcContext, tapTable).setVersion(oracleVersion).write(tapRecordEvents, writeListResultConsumer);
    }

    //create table with info which comes from tapTable
    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        if (oracleJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            return;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE \"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" + CommonSqlMaker.buildColumnDefinition(tapTable);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            oracleJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (oracleJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                oracleJdbcContext.execute("TRUNCATE TABLE \"" + oracleConfig.getSchema() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (oracleJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                oracleJdbcContext.execute("DROP TABLE \"" + oracleConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\"";
        oracleJdbcContext.query(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        OracleOffset oracleOffset;
        //beginning
        if (null == offsetState) {
            oracleOffset = new OracleOffset(CommonSqlMaker.getOrderByUniqueKey(tapTable), 0L);
        }
        //with offset
        else {
            oracleOffset = (OracleOffset) offsetState;
        }
        String sql = "SELECT * FROM (SELECT a.*,ROWNUM row_no FROM\"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\" a " + oracleOffset.getSortString() + ") WHERE row_no>" + oracleOffset.getOffsetValue();
        oracleJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && !resultSet.isAfterLast() && resultSet.getRow() > 0) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    oracleOffset.setOffsetValue(oracleOffset.getOffsetValue() + eventBatchSize);
                    eventsOffsetConsumer.accept(tapEvents, oracleOffset);
                    tapEvents = list();
                }
                resultSet.next();
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                oracleOffset.setOffsetValue(oracleOffset.getOffsetValue() + tapEvents.size());
            }
            eventsOffsetConsumer.accept(tapEvents, oracleOffset);
        });

    }

    //one filter can only match one record
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + oracleConfig.getSchema() + "\".\"" + tapTable.getId() + "\" WHERE " + CommonSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                oracleJdbcContext.query(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM \"" + oracleConfig.getSchema() + "\".\"" + table.getId() + "\" " + CommonSqlMaker.buildSqlByAdvanceFilter(filter);
        oracleJdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
                resultSet.next();
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = oracleJdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
            List<DataMap> columnList = oracleJdbcContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = oracleJdbcContext.queryAllIndexes(subTableNames);
            subList.forEach(subTable -> {
                //2、table name/comment
                String table = subTable.getString("TABLE_NAME");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("COMMENTS"));
                //3、primary key and table index
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("TABLE_NAME")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("INDEX_NAME"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v -> ((BigDecimal) v.get("IS_PK")).intValue() == 1)) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("COLUMN_NAME")).collect(Collectors.toList()));
                    }
                    TapIndex index = new TapIndex();
                    index.setName(key);
                    List<TapIndexField> fieldList = TapSimplify.list();
                    value.forEach(v -> {
                        TapIndexField field = new TapIndexField();
                        field.setFieldAsc("ASC".equals(v.getString("DESCEND")));
                        field.setName(v.getString("COLUMN_NAME"));
                        fieldList.add(field);
                    });
                    index.setUnique(value.stream().anyMatch(v -> "UNIQUE".equals(v.getString("UNIQUENESS"))));
                    index.setPrimary(value.stream().anyMatch(v -> ((BigDecimal) v.get("IS_PK")).intValue() == 1));
                    index.setIndexFields(fieldList);
                    tapIndexList.add(index);
                });
                //4、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("TABLE_NAME")))
                        .forEach(col -> {
                            TapField tapField = new OracleColumn(col).getTapField();
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
        oracleConfig = new OracleConfig().load(connectionContext.getConnectionConfig());
        OracleTest oracleTest = new OracleTest(oracleConfig);
        TestItem testHostPort = oracleTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return;
        }
        TestItem testConnect = oracleTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return;
        }
        oracleTest.close();
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return oracleJdbcContext.queryAllTables(null).size();
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if (EmptyKit.isNull(cdcRunner)) {
            cdcRunner = new OracleCdcRunner().useConfig(oracleConfig)
                    .init(
                            tableList,
                            offsetState,
                            recordSize,
                            consumer
                    );
        }
        cdcRunner.startCdcRunner();
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return new OracleOffset();
    }
}
