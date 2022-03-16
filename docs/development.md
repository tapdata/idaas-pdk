# Development Guide

## Develop your own data source connector
### Use IntelliJ IDEA to open idaas-pdk
### We leave empty methods for you to fill up 

![This is an image](images/sourceStateDiagram.jpg)
![This is an image](images/targetStateDiagram.jpg)
Method invocation life circle

    init
        if(batchEnabled) {
            batchCount  
            batchRead
        }
        while(streamEnabled)
            streamRead
    destroy

## After development
    cd yourConnector
    mvn package

## Register your connector into Tapdata
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
