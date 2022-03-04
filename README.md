# Installation

Maintainer: @Berry

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
	
	
