# Test Driven Development


## Introduction
Generated PDK connector project has several unit tests. Every time execute mvn package, the tests will be executed to test the PDK connector is a qualified implementation. 

When register PDK connector to Tapdata, the connector must pass these unit tests before actually uploaded to Tapdata. 

## Test requirement
Provide the connection.json under src/test/resources/config directory, which is the value of "configOptions" in connector json file. 

For example
If the connector json file define the "configOptions" like below, 

```json
{
  "configOptions": {
    "type": "object",
    "properties": {
      "token": {
        "type": "string",
        "title": "API Token",
        "x-decorator": "FormItem",
        "x-component": "Input"
      },
      "spaceId": {
        "type": "string",
        "title": "Space ID",
        "x-decorator": "FormItem",
        "x-component": "Input"
      }
    }
  }
}
```
Then the connection.json under src/test/resources/config directory should be like, 
```json
{
  "connectionConfig": {
    "token": "uskMiSCZAbukcGsqOfRqjZZ",
    "spaceId": "spcvyGLrtcYgs"
  },
  "table" : {
    "id" : "FromTablesSource"
  }
}
```
"table" is the table id our unit tests test against.

"ConnectionTestTest" and "DiscoverSchemaTest" is general unit tests, so "table" key will not be used.

## Unit tests
Below are the unit tests that every PDK connector has to pass.  
* ConnectionTestTest
    - Precondition:
        - connection.json, "table" key in connection.json will not be used. 
    - Test steps: 
        - connectionTest method return at least on successfully Test Item or return list of non-failed Test Items
    
* DiscoverSchemaTest
    - Precondition:
      - connection.json, "table" key in connection.json will not be used.
    - Test steps:
      - discoverSchema method return at least one table. 
    
* BatchOffsetTest
    - Precondition:
        - connection.json, prepare empty table
        - batch read only      
        - writeRecord method is implemented
    - Test steps:
        - batchCount, make sure count is 0, if not, clearTable to remove all records
        - create another job, insert 10 records
        - prepare a dag job, flow records into tdd-connector target
        - batchRead with batchSize is 6, first time batchRead record list size should be 6
        - job stop, call batchOffset method to save batch offset
        - job start again, given the recovered offset
        - batchCount with recovered offset, return 4 or throw NotSupportedException, batchCount with null offset to redo again
        - if supported, batchRead with recovered write 4 records into tdd connect target
        - if not support, batchRead write 10 records into tdd connector target
        - create another job, delete the 10 records
        - batchCount, make sure count is 0 again
        
* StreamOffsetTest
    - Precondition:
        - connection.json, prepare empty table
        - batch read only      
        - writeRecord method is implemented
    - Test steps:
        - batchCount, make sure count is 0, if not, clearTable to remove all records
        - prepare a dag job, flow records into tdd-connector target        
        - enter stream read state  
        - create another job, insert 3 records, update 2, delete 1
        - tdd connector target verify the stream events are correct
        - stop job, call streamOffset method to save stream offset
        - create another job, insert 9 records, update 2, delete 1
        - job start again, given the recovered stream offset
        - enter stream read state
        - tdd connector target verify the stream events are correct
        - create another job, delete the 10 records
        - batchCount, make sure count is 0 again  
        
* CreateTableWithOpenTypesTest (Not ready yet)
        
* SourceTest
    - Precondition:
        - connection.json, prepare empty table
        - writeRecord method is implemented
        - enable the batch and stream
    - Test steps:
        - batchCount, make sure count is 0, if not, clearTable to remove all records
        - create another job, insert 10 records with many fields cover all the TapTypes
        - prepare a dag job, flow records into tdd-connector target
        - batchRead with batchSize is 6, every time batchRead record list size can not exceed 6
        - batchRead second time, all 10 records have been inserted into tdd connector target
        - tdd connector target verify all records with correct values and types
        - enter stream state
        - create another job, insert one, update one, delete one
        - tdd connector target verify the stream events are correct
        - create another job, delete the 10 records
        - batchCount , make sure count is 0 again   
  
* TargetTest
    - Precondition:
        - connection.json, prepare empty table
        - queryByFilter method is implemented
    - Test steps: 
        - batchCount, make sure count is 0, if not, clearTable to remove all records
        - prepare a dag job, use tdd-connector source insert 10 records (with many fields cover all the TapTypes) into current connector
        - use queryByFilter to verify the 10 records are correct values and types
        - tdd-connector source update 5 records in 10
        - use queryByFilter to verify the 5 records are update correctly
        - tdd-connector source delete the 10 records  
        - use queryByFilter to verify the 10 records are deleted
        - batchCount , make sure count is 0 again
