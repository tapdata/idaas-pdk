package io.tapdata.connector.doris;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.doris.utils.DorisConfig;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.ddl.table.TapAlterTableEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.value.*;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.impl.JsonParserImpl;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class DorisConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = DorisConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
    private final JsonParser jsonParser = new JsonParserImpl();
    private DorisConfig dorisConfig;
    private Connection conn;
    private Statement stmt;
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    private void initConnection(DefaultMap config) {
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
        //TODO Load schema from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables with specified fields.
//        consumer.accept(list(
//                //Define first table
//                table("empty-table1")
//                        //Define a field named "id", origin field type, whether is primary key and primary key position
//                        .add(field("id", "VARCHAR").isPrimaryKey(true).partitionKeyPos(1))
//                        .add(field("description", "TEXT"))
//                        .add(field("name", "VARCHAR"))
//                        .add(field("age", "DOUBLE")),
//                //Define second table
//                table("empty-table2")
//                        .add(field("id", "VARCHAR").isPrimaryKey(true).partitionKeyPos(1))
//                        .add(field("description", "TEXT"))
//                        .add(field("name", "VARCHAR"))
//                        .add(field("age", "DOUBLE"))
//        ));
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
        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO execute read test here
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test here
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        //Read log test to check CDC capability
        //TODO execute read log test here
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));

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
                String originType = tapField.getOriginType();
                if (value instanceof DateTime && ("datetime".equals(originType) || "date".equals(originType))) {
                    TimeZone timeZone = ((DateTime) value).getTimeZone();
                    if(timeZone!=null) simpleDateFormat.setTimeZone(timeZone);
                    builder.append("'").append(simpleDateFormat.format(new Date(((DateTime) value).getSeconds() * 1000L))).append("'").append(',');
                } else {
                    builder.append("'").append(value).append("'").append(',');
                }

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


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        //TODO write records into database
        initConnection(connectorContext.getConnectionConfig());
        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count


        TapTable tapTable;
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            tapTable = connectorContext.getTable();
            ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tapTable.getName(), new String[]{"TABLE"});
            if (!table.first()) throw new RuntimeException("Table " + tapTable.getName() + " not exist!");
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                String sql = "INSERT INTO " + tapTable.getName() + " VALUES (" + buildColumnValues(tapTable, after) + ")";
                stmt.execute(sql);
                inserted.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                updated.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                deleted.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
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
        //TODO release resources
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
        isShutDown.set(true);
    }
}
