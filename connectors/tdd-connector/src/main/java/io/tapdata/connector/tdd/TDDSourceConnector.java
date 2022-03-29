package io.tapdata.connector.tdd;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapAlterTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.value.TapStringValue;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("sourceSpec.json")
public class TDDSourceConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = TDDSourceConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
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
        //TODO originType最好使用标准列类型来表达， 避免混淆
        consumer.accept(list(
                //Define first table
                table("tdd-table")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "tapString").isPrimaryKey(true).primaryKeyPos(2))
                        .add(field("tddUser", "tapString"))
                        .add(field("tapString", "tapString").primaryKeyPos(1))
                        .add(field("tapString10", "tapString(10)"))
                        .add(field("tapString10Fixed", "tapString(10) fixed"))
                        .add(field("tapInt", "int"))
                        .add(field("tapBoolean", "tapBoolean"))
                        .add(field("tapDate", "tapDate"))
                        .add(field("tapArrayString", "tapArray"))
                        .add(field("tapArrayDouble", "tapArray"))
                        .add(field("tapArrayTDDUser", "tapArray"))
                        .add(field("tapRawTDDUser", "tapRaw"))
                        .add(field("tapNumber", "tapNumber"))
//                        .add(field("tapNumber8", "tapNumber(8)"))
                        .add(field("tapNumber52", "tapNumber(5, 2)"))
                        .add(field("tapBinary", "tapBinary"))
                        .add(field("tapTime", "tapTime"))
                        .add(field("tapMapStringString", "tapMap"))
                        .add(field("tapMapStringDouble", "tapMap"))
                        .add(field("tapMapStringTDDUser", "tapMap"))
                        .add(field("tapDateTime", "tapDateTime"))
                        .add(field("tapDateTimeTimeZone", "tapDateTime"))
        ));
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        //TODO execute connection test here
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
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
 //        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     *
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportWriteRecord(this::writeRecord);

        codecRegistry.registerToTapValue(TDDUser.class, value -> new TapStringValue(toJson(value)));

        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportBatchOffset(this::batchOffset);
//        connectorFunctions.supportStreamOffset(this::streamOffset);
        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);
    }

    private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
        PDKLogger.info(TAG, "createTable");
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(needCreateTable)
     *      createTable
     *  if(needClearTable)
     *      clearTable
     *  writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        //TODO write records into database

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for(TapRecordEvent recordEvent : tapRecordEvents) {
            if(recordEvent instanceof TapInsertRecordEvent) {
                inserted.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapUpdateRecordEvent) {
                updated.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapDeleteRecordEvent) {
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
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @return
     */
    private long batchCount(TapConnectorContext connectorContext, Object offset) {
        //TODO Count the batch size.
        return 20L;
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, int batchSize, Consumer<List<TapEvent>> tapReadOffsetConsumer) {
        //TODO batch read all records from database, use consumer#accept to send to flow engine.

        //Below is sample code to generate records directly.
        for (int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < 18; i++) {
                TapInsertRecordEvent recordEvent = insertRecordEvent(map(
                        entry("id", "id_" + counter.incrementAndGet()),
                        entry("tddUser", new TDDUser("uid_" + counter.get(), "name_" + counter.get(), "desp_" + counter.get(), (int) counter.get(), TDDUser.GENDER_FEMALE)),
                        entry("tapString", "123"),
                        entry("tapString10", "1234567890"),
                        entry("tapString10Fixed", "1"),
                        entry("tapInt", 123123),
                        entry("tapBoolean", true),
                        entry("tapDate", new Date()),
                        entry("tapArrayString", Arrays.asList("1", "2", "3")),
                        entry("tapArrayDouble", Arrays.asList(1.1, 2.2, 3.3)),
                        entry("tapArrayTDDUser", Arrays.asList(new TDDUser("a", "n", "d", 1, TDDUser.GENDER_MALE), new TDDUser("b", "a", "b", 2, TDDUser.GENDER_FEMALE))),
                        entry("tapRawTDDUser", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)),
                        entry("tapNumber", 1233),
//                        entry("tapNumber(8)", 1111),
                        entry("tapNumber52", 343.22),
                        entry("tapBinary", new byte[]{123, 21, 3, 2}),
                        entry("tapTime", new Date()),
                        entry("tapMapStringString", new HashMap<String, String>() {{put("a", "a");put("b", "b");}}),
                        entry("tapMapStringDouble", new HashMap<String, Double>() {{put("a", 1.0);put("b", 2.0);}}),
                        entry("tapMapStringTDDUser", new HashMap<String, TDDUser>() {{put("a", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE));}}),
                        entry("tapDateTime", new Date()),
                        entry("tapDateTimeTimeZone", new Date())
                ), connectorContext.getTable());
                tapEvents.add(recordEvent);

                TapUpdateRecordEvent updateDMLEvent = updateDMLEvent(map(
                        entry("id", "id_1"),
                        entry("tapString", "123")
                ), map(
                        entry("id", "id_1"),
                        entry("tddUser", new TDDUser("uid_" + counter.get(), "name_" + counter.get(), "desp_" + counter.get(), (int) counter.get(), TDDUser.GENDER_FEMALE)),
                        entry("tapString", "123123123123"),
                        entry("tapString10", "1234567890000"),
                        entry("tapString10Fixed", "10000"),
                        entry("tapInt", 321321),
                        entry("tapBoolean", false),
                        entry("tapDate", new Date()),
                        entry("tapArrayString", Arrays.asList("3", "2", "1")),
                        entry("tapArrayDouble", Arrays.asList(6.1, 5.2, 4.3)),
                        entry("tapArrayTDDUser", Arrays.asList(new TDDUser("b", "a", "b", 1, TDDUser.GENDER_MALE), new TDDUser("a", "n", "d", 2, TDDUser.GENDER_FEMALE))),
                        entry("tapRawTDDUser", new TDDUser("b1", "a1", "b1", 22, TDDUser.GENDER_MALE)),
                        entry("tapNumber", 3221),
                        //                        entry("tapNumber(8)", 1111),
                        entry("tapNumber52", 22.343),
                        entry("tapBinary", new byte[]{2, 3, 21, 123}),
                        entry("tapTime", new Date()),
                        entry("tapMapStringString", new HashMap<String, String>() {{
                            put("c", "c");
                            put("d", "d");
                        }}),
                        entry("tapMapStringDouble", new HashMap<String, Double>() {{
                            put("c", 3.0);
                            put("d", 4.0);
                        }}),
                        entry("tapMapStringTDDUser", new HashMap<String, TDDUser>() {{
                            put("b", new TDDUser("b1", "a1", "b1", 22, TDDUser.GENDER_MALE));
                        }}),
                        entry("tapDateTime", new Date()),
                        entry("tapDateTimeTimeZone", new Date())
                ), connectorContext.getTable());
                tapEvents.add(updateDMLEvent);
                TapDeleteRecordEvent deleteRecordEvent = deleteDMLEvent(map(
                        entry("id", "id_1")
                ), connectorContext.getTable());
                tapEvents.add(deleteRecordEvent);


                List<String> deleteFields = new ArrayList<>();
                List<TapField> insertFields = new ArrayList<>();
                Map<String, TapField> changedNameFields = new HashMap<>();
                deleteFields.add("tapString");
                // TODO insertFields field 推演
//            insertFields.add(new TapField("addStringField", "tapString").defaultValue("default_value"));
                insertFields.add(new TapField("addStringField", "VARCHAR(256)").defaultValue("test"));
                TapField renameTapField = connectorContext.getTable().getNameFieldMap().get("tapString10").clone();
                renameTapField.setName("renameTapString10");
                changedNameFields.put(renameTapField.getName(), renameTapField);

                TapAlterTableEvent tapAlterTableEvent = new TapAlterTableEvent();
                tapAlterTableEvent.setTable(connectorContext.getTable());
                tapAlterTableEvent.setChangedNameFields(changedNameFields);
                tapAlterTableEvent.setDeletedFields(deleteFields);
                tapAlterTableEvent.setInsertFields(insertFields);
                tapEvents.add(tapAlterTableEvent);
            }

            TapUpdateRecordEvent updateDMLEvent = updateDMLEvent(map(
                        entry("id", "id_1"),
                        entry("tapString", "123")
                    ), map(
                        entry("id", "id_1"),
                        entry("tddUser", new TDDUser("uid_" + counter.get(), "name_" + counter.get(), "desp_" + counter.get(), (int) counter.get(), TDDUser.GENDER_FEMALE)),
                        entry("tapString", "123123123123"),
                        entry("tapString10", "1234567890"),
                        entry("tapString10Fixed", "1"),
                        entry("tapInt", 123123),
                        entry("tapBoolean", true),
                        entry("tapDate", new Date()),
                        entry("tapArrayString", Arrays.asList("1", "2", "3")),
                        entry("tapArrayDouble", Arrays.asList(1.1, 2.2, 3.3)),
                        entry("tapArrayTDDUser", Arrays.asList(new TDDUser("a", "n", "d", 1, TDDUser.GENDER_MALE), new TDDUser("b", "a", "b", 2, TDDUser.GENDER_FEMALE))),
                        entry("tapRawTDDUser", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)),
                        entry("tapNumber", 1233),
    //                        entry("tapNumber(8)", 1111),
                        entry("tapNumber52", 343.22),
                        entry("tapBinary", new byte[]{123, 21, 3, 2}),
                        entry("tapTime", new Date()),
                        entry("tapMapStringString", new HashMap<String, String>() {{put("a", "a");put("b", "b");}}),
                        entry("tapMapStringDouble", new HashMap<String, Double>() {{put("a", 1.0);put("b", 2.0);}}),
                        entry("tapMapStringTDDUser", new HashMap<String, TDDUser>() {{put("a", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE));}}),
                        entry("tapDateTime", new Date()),
                        entry("tapDateTimeTimeZone", new Date())
            ), connectorContext.getTable());
            tapEvents.add(updateDMLEvent);

            TapDeleteRecordEvent deleteRecordEvent = deleteDMLEvent(map(
                    entry("id", "id_2"),
                    entry("tapString", "123")
            ), connectorContext.getTable());
            tapEvents.add(deleteRecordEvent);
            tapReadOffsetConsumer.accept(tapEvents);
        }
        counter.set(counter.get() + 1000);
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> destroy -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param consumer
     */
    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
        //TODO using CDC APi or log to read stream records from database, use consumer#accept to send to flow engine.

        //Below is sample code to generate stream records directly
        while (!isShutDown.get()) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < 10; i++) {
                TapInsertRecordEvent event = insertRecordEvent(map(
                        entry("id", counter.incrementAndGet()),
                        entry("description", "123"),
                        entry("name", "123"),
                        entry("age", 12)
                ), connectorContext.getTable());
                tapEvents.add(event);
            }

            sleep(1000L);
            consumer.accept(tapEvents);
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
        //TODO release resources
        isShutDown.set(true);
    }
}
