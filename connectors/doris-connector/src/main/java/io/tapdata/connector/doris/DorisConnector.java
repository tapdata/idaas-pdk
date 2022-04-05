package io.tapdata.connector.doris;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.doris.bean.DorisColumn;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class DorisConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = DorisConnector.class.getSimpleName();
    private DorisConfig dorisConfig;
    private Connection conn;
    private Statement stmt;
    private static final String TABLE_COLUMN_NAME = "TABLE";


    private void initConnection(DataMap config) {
        try {
            if (conn == null) {
                if (dorisConfig == null) dorisConfig = DorisConfig.load(config);
                String dbUrl = dorisConfig.getDatabaseUrl();
                Class.forName(dorisConfig.getJdbcDriver());
                conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
            }
            if (stmt == null) stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Connection Failed!");
        }

    }

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Flow engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        dorisConfig = DorisConfig.load(connectionContext.getConnectionConfig());
        String dbUrl = dorisConfig.getDatabaseUrl();
        conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());

        List<TapTable> tapTableList = new LinkedList<>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), dorisConfig.getDatabase(), null, new String[]{TABLE_COLUMN_NAME});
        while (tableResult.next()) {
            String tableName = tableResult.getString("TABLE_NAME");
            // TODO 暂时无法通过jdbc获取键信息
            TapTable table = table(tableName);
            ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), dorisConfig.getDatabase(), tableName, null);
            while (columnsResult.next()) {
                TapField tapField = new DorisColumn(columnsResult).getTapField();
                table.add(tapField);
            }
            tapTableList.add(table);
        }
        consumer.accept(tapTableList);
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        initConnection(connectionContext.getConnectionConfig());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO 通过权限检查有没有读权限, 暂时无法通过jdbc方式获取权限信息
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO 通过权限检查有没有写权限, 暂时无法通过jdbc方式获取权限信息
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        //When test failed
        // consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
        // consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
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
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "boolean", tapValue -> {
            if (tapValue != null) {
                Boolean value = tapValue.getValue();
                if (value != null && value) {
                    return 1;
                }
            }
            return 0;
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "datetime", tapValue -> {
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
            String sql = "SELECT * FROM " + tapTable.getName() + " WHERE " + buildKeyAndValue(tapTable, filter.getMatch(), "AND");
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
                e.printStackTrace();
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);

    }

    private String buildDistributedKey(Collection<String> primaryKeyNames) {
        StringBuilder builder = new StringBuilder();
        for (String fieldName : primaryKeyNames) {
            builder.append(fieldName);
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getOriginType() == null) continue;
            builder.append(tapField.getName()).append(' ');
            builder.append(tapField.getOriginType()).append(' ');
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            } else {
                builder.append("NULL").append(' ');
            }
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ').append(tapField.getDefaultValue()).append(' ');
            }
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private Object getFieldOriginValue(TapField tapField, Object tapValue) {
        Object result = tapValue;
        if (tapValue instanceof DateTime) {
            // TODO 依据不同的TapField进行不同类型的格式化
            result = this.formatTapDateTime((DateTime) tapValue, "yyyy-MM-dd HH:mm:ss");
        }
        return result;
    }

    private String buildInsertKeyAndValues(TapTable tapTable, Map<String, Object> record) {
        // 之前作为单条记录插入使用 insert into table values ([buildInsertKeyAndValues])
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = record.get(columnName);
            if (tapField.getOriginType() == null) continue;
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    builder.append("\'").append(tapField.getDefaultValue()).append("'").append(',');
                } else {
                    builder.append("null").append(',');
                }
            } else {
                builder.append("'").append(getFieldOriginValue(tapField, tapValue)).append("'").append(',');
            }
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private void addBatchInsertRecord(TapTable tapTable, Map<String, Object> insertRecord, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.clearParameters();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int pos = 1;
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = insertRecord.get(columnName);
            if (tapField.getOriginType() == null) continue;
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    preparedStatement.setObject(pos, tapField.getDefaultValue());
                } else {
                    preparedStatement.setObject(pos, null);
                }
            } else {
                preparedStatement.setObject(pos, getFieldOriginValue(tapField, tapValue));
            }
            pos += 1;
        }
        preparedStatement.addBatch();
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String sql = "CREATE TABLE " + tapTable.getName() +
                "(" + this.buildColumnDefinition(tapTable) + ")" +
                "UNIQUE KEY (" + this.buildDistributedKey(primaryKeys) + " ) " +
                "DISTRIBUTED BY HASH(" + this.buildDistributedKey(primaryKeys) + " ) BUCKETS 10 " +
                "PROPERTIES(\"replication_num\" = \"1\")";

        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getName() + " Failed! \n ");

        }
        PDKLogger.info(TAG, "createTable");
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
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapTable.getName(), new String[]{TABLE_COLUMN_NAME});
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
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapTable.getName(), new String[]{});
            if (table.first()) {
                String sql = "DROP TABLE " + tapTable.getName();
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapTable.getName() + " Failed! \n ");
        }
    }

    private String buildBatchInsertSQL(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int fieldCount = 0;
        for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            TapField tapField = nameFieldMap.get(entry.getKey());
            if (tapField.getOriginType() == null) continue;
            fieldCount += 1;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fieldCount; i++) {
            stringBuilder.append("?, ");
        }
        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return "INSERT INTO " + tapTable.getName() + " VALUES (" + stringBuilder + ")";
    }

    private String buildKeyAndValue(TapTable tapTable, Map<String, Object> record, String splitSymbol) {
        StringBuilder builder = new StringBuilder();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String fieldName = entry.getKey();
            builder.append(fieldName).append("=").append("'").
                    append(getFieldOriginValue(nameFieldMap.get(fieldName), entry.getValue())).
                    append("' ").append(splitSymbol).append(" ");
        }
        builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        return builder.toString();
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        initConnection(connectorContext.getConnectionConfig());
        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count


        TapTable tapTable = connectorContext.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        PreparedStatement preparedStatement = conn.prepareStatement(buildBatchInsertSQL(tapTable));
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapTable.getName(), new String[]{TABLE_COLUMN_NAME});
            if (!table.first()) throw new RuntimeException("Table " + tapTable.getName() + " not exist!");
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                addBatchInsertRecord(tapTable, after, preparedStatement);
                inserted.incrementAndGet();
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                executeBatchInsert(preparedStatement);
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> filterAfter = new LinkedHashMap<>();
                Map<String, Object> updateAfter = new LinkedHashMap<>();
                for (Map.Entry<String, Object> entry : after.entrySet()) {
                    String fieldName = entry.getKey();
                    if (nameFieldMap.get(fieldName).getPrimaryKey() != null && nameFieldMap.get(fieldName).getPrimaryKey()) {
                        filterAfter.put(fieldName, entry.getValue());
                    } else {
                        updateAfter.put(fieldName, entry.getValue());
                    }
                }
                String sql = "UPDATE " + tapTable.getName() +
                        " SET " + buildKeyAndValue(tapTable, updateAfter, ",") +
                        " WHERE " + buildKeyAndValue(tapTable, filterAfter, "AND");
                stmt.execute(sql);
                updated.incrementAndGet();
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                executeBatchInsert(preparedStatement);
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> after = deleteRecordEvent.getBefore();
                String sql = "DELETE FROM " + tapTable.getName() + " WHERE " + buildKeyAndValue(tapTable, after, "AND");
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

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
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
