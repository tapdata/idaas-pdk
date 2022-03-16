# Development Guide

## Open types

**Open types is only required for your data source which need create table before insert records**, like MySQL, Oracle, Postgres, etc.

Open types is the mapping of data type (include capabilities) with TapType.   
TapType is the generic type definition in iDaaS Flow Engine.

* PDK connector define open types json mapping, let Flow Engine know the mapping of data types with TapTypes. 
* Records with TapTypes will flow into Flow Engine to do processing, join, etc. 
* Once the records with TapTypes will flow into a PDK target, Flow engine will conjecture the best data types for PDK developer to create the table, base on the input of open types json mapping.  

For more about [Open type](open-type.md).

If without TapType, the conversion lines is like below, which is very complex to maintain. 
![This is an image](images/withoutTapType.png)

With TapType in the middle of type conversion, the conversion can be maintainable and the fundamental for data processing, join, etc.

![This is an image](images/withTapType.png)

Above is the important concept to implement PDK connector, especially for the data source which need create table for insert records. 

## Develop PDK connector

There are 11 methods to implement. The more developer implement, the more features that your connector provides. 
* PDK Source methods to implement
    - BatchCount (must as a source)
        - Return the total record size for batch read. 
    - BatchOffset
        - Return current batch offset, PDK developer define what is batch offset. Batch offset will be provided in batch read method when recover the batch read.  
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


## After development
```shell
  cd yourConnector
  mvn package
```
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
    ./bin/tap register -a 3324cfdf-7d3e-4792-bd32-571638d4562f -t http://192.168.1.126:3004 dist/your-connector-v1.0-SNAPSHOT.jar

## Now you can use your PDK connector in Tapdata website

# Project structure

## Modules
    connectors 
    //Parent module to put all the connectors below.
        connector-core 
        //Common core is the parent module for every connector module
        empty-connector 
        //Empty connector to output dummy records
        file-connector 
        //File connector to append record in user specified local file
        mongodb-connector 
        //Mongodb connector, not finished
        vika-connector 
        //Vika connector, support batchRead and write to a datasheet. API token and spaceId must be specified in connector form
    tapdata-pdk-api 
    //PDK API, every connector depend on the API
    tapdata-pdk-cli 
    //Run PDK in CLI, like register to Tapdata, test methods, etc
    tapdata-pdk-runner 
    //Provide integration API to Tapdata FlowEngine, also provide a tiny flow engine for test purpose

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
