# iDaaS PDK

![GitHub stars](https://img.shields.io/github/stars/tapdata/idaas-pdk?style=social&label=Star&maxAge=2592000)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)


## What is iDaaS

Data as a service (DaaS) is a data management strategy that uses the cloud(or centralized data platform) to deliver data storage, integration, processing, and servicing capabilities in an on demand fashion.

Tapdata iDaaS, or Incremental Data as a Service, is an open source implementation of the DaaS architecture that puts focus on the incremental updating capability of the data platform.  In essense, iDaaS provides following capabilities:


- Searchable data catalog, allows you to collect/manage/organize and search for all your data assets across your organization

- "Any to Any" real time data integration platform, allows you to move data from source systems to DaaS or to destinations like data warehouses or kafka

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

PDK connectors provide data source and target into iDaaS Flow Engine.


## Quick Start
how simple to development
```java
@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase implements TapConnector {
    
}
```

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
}

```

## Installation

**[How to install?](docs/deployment.md)**

## Open type
[Open type](docs/open-type.md)

## Development Guide

## Test Driven Development

## PDK Registration

