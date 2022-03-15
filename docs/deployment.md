# Installation


## Requirements
	install java 8 and above
    install maven

## Install from source

	git clone https://github.com/tapdata/idaas-pdk.git
    cd idaas-pdk
    mvn clean install

## Create connector project 

	./bin/tap template --group io.tapdata --name MyDataSourceConnector --version 0.0.1 --output ./connectors
