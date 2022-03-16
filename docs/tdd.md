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
    - Input:
        - connection.json, "table" key in connection.json will not be used. 
    - Expect: 
        - connectionTest method return at least on successfully Test Item or return list of non-failed Test Items
* DiscoverSchemaTest
    - Input:
      - connection.json, "table" key in connection.json will not be used.
    - Expect:
      - discoverSchema method return at least one table. 
* BatchOffsetTest
* StreamOffsetTest
* CreateTableWithOpenTypesTest
* DifferentStructureDatabaseTest
* SameStructureDatabaseTest
