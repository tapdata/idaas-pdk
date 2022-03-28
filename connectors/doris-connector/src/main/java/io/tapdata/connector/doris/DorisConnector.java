package io.tapdata.connector.doris;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.doris.bean.DorisColumn;
import io.tapdata.connector.doris.utils.DorisConfig;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.ddl.table.TapAlterTableEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.value.*;
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


    private void initConnection(DataMap config) {
        if (conn == null) {
            try {
                if (dorisConfig == null) dorisConfig = DorisConfig.load(config);
                String dbUrl = dorisConfig.getDatabaseUrl();
                Class.forName(dorisConfig.getJdbcDriver());
                conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
                stmt = conn.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Create Connection Failed!");
            }

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
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        try {
            dorisConfig = DorisConfig.load(connectionContext.getConnectionConfig());
            String dbUrl = dorisConfig.getDatabaseUrl();
            conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("Create Connection Failed!");
        }

        List<TapTable> tapTableList = new LinkedList<>();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), dorisConfig.getDatabase(), null, new String[]{"TABLE"});
            while (tableResult.next()) {
                String tableName = tableResult.getString("TABLE_NAME");
                TapTable table = table(tableName);
                ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), dorisConfig.getDatabase(), tableName, null);
                while (columnsResult.next()) {
                    TapField tapField = new DorisColumn(columnsResult).getTapField();
                    table.add(tapField);
                }
                tapTableList.add(table);
            }
            consumer.accept(tapTableList);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("discoverSchema Failed! Execute SQL Failed!");
        }
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
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        initConnection(connectionContext.getConnectionConfig());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO 通过权限检查有没有读权限
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO 通过权限检查有没有写权限
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
        connectorFunctions.supportAlterTable(this::alterTable);
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
//        TapTable tapTable = connectorContext.getTable();
//        Map<String, Object> filterMap = new LinkedHashMap<>();
//        for (TapFilter filter : filters) {
//            filterMap.put(filter.getMatch().get())
//        }
//        String sql = "SELECT * FROM " + tapTable.getName() + " WHERE " + buildKeyAndValue(tapTable, after, "AND");
//
//        //TODO 实现一下这个方法， TDD测试会要求实现这个方法做数据验证。 基于多个filter查询返回多个数据。 filter就是精确匹配的， 会用primaryKeys组织一个Map。
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

    private Object getFieldValue(TapField tapField, Object originValue) {
        Object result = originValue;
        if (originValue instanceof DateTime) {
            // TODO 依据不同的TapField进行不同类型的格式化
            String dateValue = this.formatTapDateTime((DateTime) originValue, "yyyy-MM-dd HH:mm:ss");
        }
        return result;
    }

    private String buildColumnValues(TapTable tapTable, Map<String, Object> record) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            Object value = record.get(columnName);
            if (tapField.getOriginType() == null) continue;
            if (value == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    builder.append("\'").append(tapField.getDefaultValue()).append("'").append(',');
                } else {
                    builder.append("null").append(',');
                }
            } else {
                builder.append("'").append(getFieldValue(tapField, value)).append("'").append(',');
            }
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapConnectorContext.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String sql = "CREATE TABLE " + tapTable.getName() +
                "(" + this.buildColumnDefinition(tapTable) + ")" +
                "DISTRIBUTED BY HASH(" + this.buildDistributedKey(primaryKeys) + ") BUCKETS 10 " +
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

    private void alterTable(TapConnectorContext tapConnectorContext, TapAlterTableEvent tapAlterTableEvent) {
        PDKLogger.info(TAG, "alterTable");
        //TODO 需要实现修改表的功能， 不过测试只能先从源端模拟一个修改表事件
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        initConnection(tapConnectorContext.getConnectionConfig());
        TapTable tapTable = tapClearTableEvent.getTable();
        try {
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapClearTableEvent.getTable().getName(), new String[]{"TABLE"});
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
        TapTable tapTable = tapDropTableEvent.getTable();
        try {
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapDropTableEvent.getTable().getName(), new String[]{"TABLE"});
            if (table.first()) {
                String sql = "DROP TABLE " + tapTable.getName();
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapTable.getName() + " Failed! \n ");
        }

    }

    private String buildKeyAndValue(TapTable tapTable, Map<String, Object> record, String splitSymbol) {
        StringBuilder builder = new StringBuilder();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String fieldName = entry.getKey();
            builder.append(fieldName).append("=").append("'").
                    append(getFieldValue(nameFieldMap.get(fieldName), entry.getValue())).
                    append("' ").append(splitSymbol).append(" ");
        }
        builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        return builder.toString();
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        initConnection(connectorContext.getConnectionConfig());
        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count


        TapTable tapTable = connectorContext.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (TapRecordEvent recordEvent : tapRecordEvents) {

            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapTable.getName(), new String[]{"TABLE"});
            if (!table.first()) throw new RuntimeException("Table " + tapTable.getName() + " not exist!");
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                String sql = "INSERT INTO " + tapTable.getName() + " VALUES (" + buildColumnValues(tapTable, after) + ")";
                stmt.execute(sql);
                inserted.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
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
                //TODO Doris目前Update语句只支持在Unique模型上更新,tapField Unique字段目前不支持， 是否应该用主键来建UNIQUE？ 需要确认
                //     ref: https://doris.apache.org/zh-CN/sql-reference/sql-statements/Data%20Manipulation/UPDATE.html#description
//                stmt.execute(sql);
                updated.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> after = deleteRecordEvent.getBefore();
                String sql = "DELETE FROM " + tapTable.getName() + " WHERE " + buildKeyAndValue(tapTable, after, "AND");
                stmt.execute(sql);
                deleted.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }
        //Need to tell flow engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
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
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
