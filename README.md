# Installation

Maintainer: @Aplomb

## Requirements
	install java 8 and above
    install maven

## Install from source

	git clone https://github.com/tapdata/idaas-pdk.git
    cd idaas-pdk
    mvn clean install

## Create connector project 

	./bin/tap boot --group io.tapdata --name MyDataSourceConnector --version 0.0.1 --output ./connectors

## Develop your own data source connector
### Use IntelliJ IDEA to open idaas-pdk
### We leave empty methods for you to fill up 
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

#How to Test

## CLI Test (Temporary solution)
    io.tapdata.pdk.cli.ConnectionTestMain //To test connection test method
    io.tapdata.pdk.cli.DiscoverSchemaMain //To test all tables test method
    io.tapdata.pdk.cli.StoryMain //To describe a simple DAG to connect source to any target, to test that whether target ge the expected result
