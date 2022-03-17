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





# How to Test

## CLI Test (Temporary solution)
Run connectionTest method

    package io.tapdata.pdk.cli;
    
    public class ConnectionTestMain {
    
    public static void main(String... args) {
        args = new String[] {"connectionTest",
            "--id", "vika-pdk",
            "--group", "tapdata",
            "--buildNumber", "1",
            "--connectionConfig", "{'token' : 'uskMiSCZAbukcGsqOfRqjZZ', 'spaceId' : 'spcvyGLrtcYgs'}"
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

Run discoverSchema method

    package io.tapdata.pdk.cli;

    public class DiscoverSchemaMain {

        public static void main(String... args) {
            args = new String[] {"discoverSchema",
                "--id", "vika-pdk",
                "--group", "tapdata",
                "--buildNumber", "1",
                "--connectionConfig", "{'token' : 'uskMiSCZAbukcGsqOfRqjZZ', 'spaceId' : 'spcvyGLrtcYgs'}"
            };
            
            Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
        }
    }

To describe a simple DAG to connect Source to any Target, to test that whether target get the expected result

StoryMain

    package io.tapdata.pdk.cli;

    public class StoryMain {
    
        public static void main(String... args) {
            String rootPath = "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/tapdata-pdk-cli/src/main/resources/stories/";
            args = new String[]{"start",
                    rootPath + "emptyToFile.json",
            };
    
            Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
        }

    }

DAG json file

    {
        "id" : "dag1",
        "nodes" : [
            {
                "connectionConfig" : {},
                "table" : {
                    "name" : "empty-table1",
                    "id" : "empty-table1"
                },
                "id" : "s1",
                "pdkId" : "emptySource",
                "group" : "tapdata",
                "type" : "Source",
                "minBuildNumber" : 0
            },
            {
                "connectionConfig" : {
                    "folderPath" : "/Users/aplomb/dev/tapdata/AgentProjects/tmp"
                },
                "table" : {
                    "name" : "target1.txt",
                    "id" : "target1.txt"
                },
                "id" : "t2",
                "pdkId" : "fileTarget",
                "group" : "tapdata",
                "type" : "Target",
                "minBuildNumber" : 0
            }
        ],
        "dag" : [
            ["s1", "t2"]
        ],
        "jobOptions" : {
            "queueSize" : 100,
            "queueBatchSize" : 100
        }
    }
