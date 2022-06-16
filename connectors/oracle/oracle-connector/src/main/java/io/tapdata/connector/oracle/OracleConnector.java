package io.tapdata.connector.oracle;

import io.tapdata.common.DataSourcePool;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.oracle.bean.OracleColumn;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_oracle.json")
public class OracleConnector extends ConnectorBase {

    private OracleConfig oracleConfig;
    private OracleJdbcContext oracleJdbcContext;
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
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(oracleJdbcContext)) {
            oracleJdbcContext.finish(connectionContext.getId());
        }
    }

    private void onDestroy(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //need to clear resource outer
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        //target
        connectorFunctions.supportWriteRecord(this::writeRecord);
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportCreateIndex(this::createIndex);
//        //source
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
//        //query
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "CLOB", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
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
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        new OracleRecordWriter(oracleJdbcContext, tapTable).setVersion(oracleVersion).write(tapRecordEvents, writeListResultConsumer);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
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
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
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
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return oracleJdbcContext.queryAllTables(null).size();
    }
}
