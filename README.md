# iDaaS PDK

![GitHub stars](https://img.shields.io/github/stars/tapdata/idaas-pdk?style=social&label=Star&maxAge=2592000)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

## What is iDaaS

Data as a service (DaaS) is a data management strategy that uses the cloud(or centralized data platform) to deliver data
storage, integration, processing, and servicing capabilities in an on demand fashion.

Tapdata iDaaS, or Incremental Data as a Service, is an open source implementation of the DaaS architecture that puts
focus on the incremental updating capability of the data platform. In essense, iDaaS provides following capabilities:

- Searchable data catalog, allows you to collect/manage/organize and search for all your data assets across your
  organization

- "Any to Any" real time data integration platform, allows you to move data from source systems to DaaS or to
  destinations like data warehouses or kafka

- Programmable data platform, every data related tasks can be managed & programmed via API or Shell

- Code-less API generation, instantly turn your managed data asset into discoverable services

## What is PDK for iDaaS

**PDK is Plugin Development Kit for iDaaS.** Provide an easier way to develop data source connectors

* **Database connectors**
    * MySql, Oracle, Postgres, etc.
* **SaaS connectors**
    * Facebook, Salesforce, Google docs, etc.
* **Custom connectors**
    * Connect your custom data sources.

PDK connectors provide data source to flow in iDaaS pipeline to process, join, etc, and flow in a target which is also
provided by PDK connectors.

[comment]: <> (![This is an image]&#40;docs/images/pdkFlowDiagram.gif&#41;)

## Quick Start

Java 8+ and Maven need to be installed.

Generate Java Sample Project, then start filling the necessary methods to implement a PDK connector.

```java

@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        //TODO Load schema from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables with specified fields.
        consumer.accept(list(
                //Define first table
                table("empty-table1")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "VARCHAR").isPrimaryKey(true).partitionKeyPos(1))
                        .add(field("description", "TEXT"))
                        .add(field("name", "VARCHAR"))
                        .add(field("age", "DOUBLE"))
        ));
    }

    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
        //TODO using CDC API or log to read stream records from database, use consumer#accept to send to flow engine.
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

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        //TODO write records into database
        //Need to tell flow engine the write result
        consumer.accept(writeListResult()
                .insertedCount(tapRecordEvents.size()));
    }
}
```

Each connector need to provide a json file like above, described in TapConnector annotation.

spec.json describes three types information,

* properties
    - Connector name, icon and id.
* [configOptions](docs/configOptions.md)
    - Describe a form for user to input the information for how to connect and which database to open.
* [dataTypes](docs/dataTypes.md) (Not ready yet)
    - This will be required when current data source need create table with proper types before insert records.
      Otherwise, this key can be empty.
    - Describe the capability of types for current data source.
    - iDaaS Flow Engine will generate proper types when table creation is needed.

```json
{
  "properties": {
    "name": "PDK Sample Connector",
    "icon": "icon.jpeg",
    "id": "pdk-sample"
  },
  "configOptions": {
    "connection": {
      "type": "object",
      "properties": {
        "token": {
          "type": "string",
          "title": "Host",
          "x-decorator": "FormItem",
          "x-component": "Input"
        },
        "spaceId": {
          "type": "string",
          "title": "Database",
          "x-decorator": "FormItem",
          "x-component": "Input"
        }
      }
    }
  },
  "dataTypes": {
    "double": {
      "bit": 64,
      "to": "TapNumber"
    },
    "decimal[($precision,$scale)]": {
      "bit": 128,
      "precision": [
        1,
        27
      ],
      "defaultPrecision": 10,
      "scale": [
        0,
        9
      ],
      "defaultScale": 0,
      "to": "TapNumber"
    }
  }
}

```



## How to create PDK java project?

Requirements
*	install java 8 and above
*   install maven

Install from source

```shell
  git clone https://github.com/tapdata/idaas-pdk.git
  cd idaas-pdk
  mvn clean install
```
Create target connector project which don't need create table before insert records
```shell
./bin/tap template --type target --group io.tapdata --name XDB --version 0.0.1 --output ./connectors
```

Create target connector project which need create table before insert records
```shell
./bin/tap template --type targetNeedTable --group io.tapdata --name XDB --version 0.0.1 --output ./connectors
```

Create source connector project
```shell
./bin/tap template --type source --group io.tapdata --name XDB --version 0.0.1 --output ./connectors
```
Use IntelliJ IDE to open idaas-pdk, you can find your project under connectors module. 
* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)


## Development Guide
PDK need developer to provide below, 
* Provide open types json mapping for data types to TapTypes.
* Provide the methods for developers to implement the corresponding source and target features.

[Get started](docs/development.md)

## Test Driven Development
PDK connector will run unit tests during maven package. Also when register to Tapdata, your connector must pass those unit tests before the register to Tapdata.
This is the way we ensure our PDK connector quality. 

[TDD tests](docs/tdd.md)

## PDK Registration
```
./bin/tap register
  Push PDK jar file into Tapdata
      [FILE...]            One ore more pdk jar files
  -a, --auth=<authToken>   Provide auth token to register
  -h, --help               TapData cli help
  -l, --latest             whether replace the latest version, default is true
  -t, --tm=<tmUrl>         Tapdata TM url
  
./bin/tap register -a token-abcdefg-hijklmnop -t http://host:port dist/your-connector-v1.0-SNAPSHOT.jar
```

## Roadmap

Check out our [Roadmap for Core](https://github.com/tapdata/idaas-pdk/milestones) and
our [Roadmap for Connectors](https://github.com/tapdata/idaas-pdk/projects/2) on GitHub. You'll see the features we're
currently working on or about to. You may also give us insights, by adding your own issues and voting for specific
features / integrations.

