# Development Guide

## Data types

**Data types is only required for your data source which need create table before insert records**, like MySQL, Oracle, Postgres, etc.

Data types is the mapping of data type (include capabilities) with TapType.   
TapType is the generic type definition in iDaaS Flow Engine.

* PDK connector define data types json mapping, let Flow Engine know the mapping of data types with TapTypes. 
* Records with TapTypes will flow into Flow Engine to do processing, join, etc. 
* Once the records with TapTypes will flow into a PDK target, Flow engine will conjecture the best data types for PDK developer to create the table, base on the input of data types json mapping.  

For more about [Data Types](dataTypes.md).

If without TapType, the conversion lines is like below, which is very complex to maintain. 
![This is an image](images/withoutTapType.png)

With TapType in the middle of type conversion, the conversion can be maintainable and the fundamental for data processing, join, etc.

![This is an image](images/withTapType.png)

Above is the important concept to implement PDK connector, especially for the data source which need create table for insert records. 

### TapType
There are 10 types of TapType. 
* TapBoolean
* TapDate
* TapArray
* TapRaw
* TapNumber
* TapBinary
* TapTime
* TapMap
* TapString
* TapDateTime

We also have 10 types of TapValue for each TapType to combine value with it's TapType.
* TapBooleanValue
* TapDateValue
* TapArrayValue
* TapRawValue
* TapNumberValue
* TapBinaryValue
* TapTimeValue
* TapMapValue
* TapStringValue
* TapDateTimeValue


## idaas-pdk modules
* **connectors**
    - **connector-core**
        - The common core is the parent module for every connector module, provide convenient API to connectors
    - **empty-connector**
        - Empty connector to output dummy records
    - **file-connector**
        - File connector to append record in user specified local file
    - **tdd-connector**
        - TDD Connector provide sample data for PDK tests
* **tapdata-pdk-api**
    - PDK API, every connector depend on the API
* **tapdata-pdk-cli**
    - Run PDK in CLI, like register to Tapdata, test methods, etc
* **tapdata-pdk-runner**
    - Provide integration API to Tapdata FlowEngine, also provide a tiny flow engine for test purpose
    
## Develop PDK connector

There are 11 methods to implement. The more developer implement, the more features that your connector provides. 
* PDK Source methods to implement
    - BatchOffset
      - Return current batch offset, PDK developer define what is batch offset. Batch offset will be provided in batch read method when recover the batch read.
        
    - BatchCount (must as a source)
        - Return the total record size for batch read.
    - BatchRead (must as a source)
        - Return the record events from batch read, once this method end, flow engine will consider batch read is finished.
    - StreamRead
        - Return the record events or ddl events from stream read, this method will always be called once it returns.       
    - StreamOffset
        - Return current stream offset the latest batch. 

* PDK Target methods to implement 
    - writeRecord (must as a target)
        - Write record events into target data source. 
    - QueryByFilter
        - Verify the record with certain filter is exists or not. if exists, then update, other wise do insert. 
    - CreateTable
        - Create the table with given conjectured data types.  
    - AlterTable
        - Alter table with conjectured data types that triggered by the stream ddl events.  
    - DropTable
        - Drop table that triggered by the stream ddl events or by user selection
    - ClearTable
        - Clear table by user selection.

Source methods invocation state diagram
![This is an image](images/sourceStateDiagram.jpg)
Target methods invocation state diagram
![This is an image](images/targetStateDiagram.jpg)
### Batch Read
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    /**
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset the offset that batch read start from
     * @param batchSize the batch size for the max record list size when consumer#accept a batch
     * @param consumer push records in Flow engine
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, int batchSize, Consumer<List<TapEvent>> consumer) {
        //TODO batch read all records from database when offset == null, use consumer#accept to send to flow engine.
        //TODO if offset != null, batch read records started from the offset condition. 
        
        //Below is sample code to generate records directly.
        for (int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < 20; i++) {
                TapInsertRecordEvent recordEvent = insertRecordEvent(map(
                        entry("id", counter.incrementAndGet()),
                        entry("description", "123"),
                        entry("name", "123"),
                        entry("age", 12)
                ), connectorContext.getTable());
                tapEvents.add(recordEvent);
            }
            consumer.accept(tapEvents);
        }
    }
}
```
### Batch Offset
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    /**
     * Record the offset for batch read. 
     * Every time consumer#accept a batch of records, need update offset here. 
     * 
     * In this case batchOffset method can return the offset in runtime.
     */
    private Object offset;
    
    /**
     * Get batch offset in runtime. 
     * If null, mean no offset, batch read start from beginning. 
     * 
     * @param connectorContext
     * @return
     */
    private Object batchOffset(TapConnectorContext connectorContext) {
        return offset;
    }
}
```
### Batch Count
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    /**
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
        //TODO if offset != null, mean batch read is recovered from a offset, the count must consider the offset condition.
        
        //if don't support count by offset condition
//        if(offset != null)
//            throw new NotSupportedException();
        
        return 20L;
    }
}
``` 
### Stream Read
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    /**
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
        //TODO if offset != null, stream read will continue from offset condition. 
        
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
            consumer.accept(tapEvents);
        }
    }
}
```

### Stream Offset
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    /**
     * Record the offset for stream read. 
     * Every time consumer#accept a batch of records, need update offset here. 
     *
     * In this case streamOffset method can return the offset in runtime.
     */
    private Object streamOffset;

    /**
     * Get stream offset in runtime. 
     * If null, mean no offset, stream read start from beginning. 
     *
     * @param offsetStartTime specify the expected start time to return the offset. If null, return current offset.
     * @param connectorContext the node context in a DAG
     */
    Object streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
        //TODO return the offset of stream read in runtime. 
        //TODO offsetStartTime != null, return offset information by start time.
        
        //If don't support return offset information by start time, need throw NotSupportedException. 
//        if(offsetStartTime != null)
//            throw new NotSupportedException();
        return streamOffset;
    }
}
```
### Register Capabilities
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
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
        //Register the 11 methods to implement. 
        //If connector don't provide the capability, need remove this method registration. 
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchOffset(this::batchOffset);
        connectorFunctions.supportStreamOffset(this::streamOffset);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        
        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);

        codecRegistry.registerToTapValue(TDDUser.class, value -> new TapStringValue(toJson(value)));
    }
}
```

### Custom Codec
PDK can recognize generic types in your records and convert them into TapValue.

To convert custom class, developer need to provide the codec for how to convert the custom class into a type of TapValue. So that different data source can be able to insert this value. 

The origin object will be stored in the field originValue of TapValue, if the target is the same data source, connector can still get the original value to insert. 

```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        //TDDUser object will be convert into json string.  
        codecRegistry.registerToTapValue(TDDUser.class, value -> new TapStringValue(toJson(value)));
    }
}
```
### Write Record
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
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
}
```

## After development
```shell
  cd yourConnector
  mvn package
```
During mvn package, [TDD](tdd.md) unit tests will be executed, developer need to fix the unit test errors before register to Tapdata. 

The PDK connector jar will be generated under idaas-pdk/dist directory. 


## Register your connector into Tapdata
```
./bin/tap register
  Push PDK jar file into Tapdata
      [FILE...]            One ore more pdk jar files
  -a, --auth=<authToken>   Provide auth token to register
  -h, --help               TapData cli help
  -l, --latest             whether replace the latest version, default is true
  -t, --tm=<tmUrl>         Tapdata TM url
```
    ./bin/tap register -a token-abcdefg-hijklmnop -t http://host:port dist/your-connector-v1.0-SNAPSHOT.jar

## Now you can use your PDK connector in Tapdata website


