package ${package};

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.error.NotSupportedException;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class ${libName}Connector extends ConnectorBase implements TapConnector {
    public static final String TAG = ${libName}Connector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> destroy -> ended
     *
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Incremental engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        //TODO Load tables from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables.
        consumer.accept(list(
                //Define first table
                table("empty-table1"),
                //Define second table
                table("empty-table2"))
        );
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> destroy -> ended
     *
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        //TODO execute connection test here
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO execute read test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
//        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a target, please implement WriteRecordFunction, QueryByFilterFunction and DropTableFunction.
     * WriteRecordFunction is to write insert/update/delete events into database.
     * QueryByFilterFunction will be used to verify written record is the same with the record query from database base on the same primary keys.
     * DropTableFunction here will be used to drop the table created by tests.
     *
     * If the database need create table before record insertion, then please implement CreateTableFunction,
     * Incremental engine will generate the data types for each field base on incoming records for CreateTableFunction to create the table.
     * </p>
     *
     * <p>
     * To be as a source, please implement BatchReadFunction, BatchCountFunction, BatchOffsetFunction, StreamReadFunction and StreamOffsetFunction, QueryByAdvanceFilterFunction.
     * If the data is schema free which can not fill TapField for TapTable in discoverSchema method, Incremental Engine will sample some records to build TapField by QueryByAdvanceFilterFunction.
     * QueryByFilterFunction is not necessary, once implemented QueryByAdvanceFilterFunction.
     * BatchReadFunction is to read initial records from beginner or offset.
     * BatchCountFunction is to count initial records from beginner or offset.
     * BatchOffsetFunction is to return runtime offset during reading initial records, if batchRead not started yet, return null.
     * StreamReadFunction is to start CDC to read incremental record events, insert/update/delete.
     * StreamOffsetFunction is to return stream offset for specified timestamp or runtime stream offset.
     * </p>
     *
     * If defined data types in spec.json is not covered all the TapValue,
     * like TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue,
     * then please provide the custom codec for missing TapValue by using codeRegistry.
     * This is only needed when database need create table before insert records.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportDropTable(this::dropTable);

        //If database need insert record before table created, then please implement the below two methods.
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportDropTable(this::dropTable);

        //If database need insert record before table created, please implement the custom codec for the TapValue that data types in spec.json didn't cover.
        //TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue
//        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
//            if (tapRawValue != null && tapRawValue.getValue() != null)
//                return toJson(tapRawValue.getValue());
//            return "null";
//        });


        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportClearTable(this::clearTable);

//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchOffset(this::batchOffset);
//        connectorFunctions.supportStreamOffset(this::streamOffset);


    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(needCreateTable)
     *      createTable
     *  if(needClearTable)
     *      clearTable
     *  if(needDropTable)
     *      dropTable
     *  writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        //TODO write records into database

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for(TapRecordEvent recordEvent : tapRecordEvents) {
            if(recordEvent instanceof TapInsertRecordEvent) {
                //TODO insert record
                inserted.incrementAndGet();
                TapLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapUpdateRecordEvent) {
                //TODO update record
                updated.incrementAndGet();
                TapLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapDeleteRecordEvent) {
                //TODO delete record
                deleted.incrementAndGet();
                TapLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }
        //Need to tell incremental engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    /**
     * The method will mainly be used by TDD tests. To verify the record has writen correctly or not.
     *
     * @param connectorContext
     * @param filters Multple fitlers, need return multiple filter results
     * @param listConsumer tell incremental engine the filter results according to filters
     */
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, Consumer<List<FilterResult>> listConsumer) throws Throwable {
        //Filter is exactly match.
        //If query by the filter, no value is in database, please still create a FitlerResult with null value in it. So that incremental engine can understand the filter has no value.

        //TODO Implement the query by filter
//        if(filters != null) {
//            List<FilterResult> filterResults = new ArrayList<>();
//            for(TapFilter filter : filters) {
//                Map<String, Object> value = primaryKeyRecordMap.get(primaryKey(connectorContext, filter.getMatch()));
//                FilterResult filterResult = new FilterResult().result(value).filter(filter);
//                filterResults.add(filterResult);
//            }
//            listConsumer.accept(filterResults);
//        }

    }

    /**
     * This method will be invoked when user selected drop table before insert records. Or TDD will use this method to drop the table created for test.
     *
     * @param connectorContext
     * @param dropTableEvent
     */
    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {

    }

    /**
     * Generate primary values as a string, used for KV database as primary key.
     * This method is only FYI.
     */
    private String primaryKey(TapConnectorContext connectorContext, Map<String, Object> value) {
        Collection<String> primaryKeys = connectorContext.getTable().primaryKeys();
        StringBuilder builder = new StringBuilder();
        for(String primaryKey : primaryKeys) {
            builder.append(value.get(primaryKey));
        }
        return builder.toString();
    }

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     *
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
