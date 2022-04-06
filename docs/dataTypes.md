# Data Types

Data Types in spec.json is to describe the capabilities of data types for Incremental Engine to conjecture the best data types to create table before record insertion. 

**If the database support insert record without table creation, like MongoDB, Kafka, etc, then please ignore this document, just leave dataTypes empty in spec.json.**

##The format to describe the data types
- key is data type expression to easily matching multiple data types with several combinations which are the same type of data. 
- value is the capabilities description for min, max value, unsigned, etc. 
 
For example of the data type expression,

int[($bit)][unsigned][zerofill] can match below types 
- int
- int(8)
- int(32) unsigned
- int(64) unsigned zerofill

By this expression, the dataTypes configuration can be simplified a lot.  


The value is a json value, below is a must, 
```json
{
  "to": "TapString"
}
```
The "to" means the data type expression will be converted to the specified TapType. 
* TapBoolean
* TapDate
* TapArray
* TapRaw
* TapNumber
* TapBinary
* TapTime
* TapMap
* TapString
* TapDateTime
* TapYear

Some TapType have extra fields
* TapNumber
```text
{ 
    "to": "TapNumber",
  
    "bit": 64, //max bit
    "defaultBit" : 10, //default bit
    
    "precision" : [1, 30], //precision range, [min, max]
    "precisionDefault" : 10, //default precision
    
    "scale" : [0, 30], //scale range, [min, max]
    "scaleDefault" : 3, //default scale
    
    "unsigned" : "unsignedEx", //alias of unsigned, different database may use different label for unsigned
    "zerofill" : "zerofill" //alias of zerofill, different database may use different label for zerofill
}
```
for example

```text
{
  "int[($bit)]": {"bit": 32, "defaultBit": 8, "to": "TapNumber"},
  "double[($precision,$scale)[unsigned]": {"precision": [1, 64], "defaultPrecision": 10, "scale": [0, 8], "defaultScale": 4, "unsigned":  "unsigned", "to":  "TapNumber"},
  "tinyint": {"bit":  8, "to":  "TapNumber"}
}

```    
* TapString
```text
{
    "byte" : "16m", //Max length of string, support string with suffix "k", "m", "g", "t", "p", also number
    "defaultByte" : "1m", //Max length of string, support string with suffix "k", "m", "g", "t", "p", also number
    "fixed" : "fixed" //Alias of fixed
}
```
for example
```text
{
  "text": {"byte":  "2m", "to":  "TapString"}, 
  "varchar[($byte)]": {"byte":  1024, "defaultByte":  128, "to":  "TapString"}
}
```
* TapBinary
```text
{
    "byte" : "16m", //Max length of binary，support string with suffix "k", "m", "g", "t", "p", also number
    "defaultByte" : "1m", //Max length of binary，support string with suffix "k", "m", "g", "t", "p", also number
    "fixed" : "fixed" //Alias of fixed
}
```
* TapDate
```text
{
    "range" : ["1000-01-01", "9999-12-31"], //Date range in format YYYY-MM-DD
    "gmt" : 0, //gmt
}
```
* TapDateTime
```text
{
    "range" : ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], //Date time range in format YYYY-MM-DD hh:mm:ss
    "gmt" : 0, //gmt
}
```
* TapTime
```text
{
    "range" : ["-838:59:59","838:59:59"], //Time range in format hh:mm:ss
    "gmt" : 0, //gmt
}
```



Example of Doris data type expression is below, 
```json
{
    "boolean":{"bit":8, "unsigned":"", "to":"TapNumber"},
    "tinyint":{"bit":8, "to":"TapNumber"},
    "smallint":{"bit":16, "to":"TapNumber"},
    "int":{"bit":32, "to":"TapNumber"},
    "bigint":{"bit":64, "to":"TapNumber"},
    "largeint":{"bit":128, "to":"TapNumber"},
    "float":{"bit":32, "to":"TapNumber"},
    "double":{"bit":64, "to":"TapNumber"},
    "decimal[($precision,$scale)]":{"precision": [1, 27], "defaultPrecision": 10, "scale": [0, 9], "defaultScale": 0, "to": "TapNumber"},
    "date":{"byte":3, "range":["0000-01-01", "9999-12-31"], "to":"TapDate"},
    "datetime":{"byte":8, "range":["0000-01-01 00:00:00","9999-12-31 23:59:59"],"to":"TapDateTime"},
    "char[($byte)]":{"byte":255, "to": "TapString", "defaultByte": 1},
    "varchar[($byte)]":{"byte":"65535", "to":"TapString"},
    "string":{"byte":"2147483643", "to":"TapString"},
    "HLL":{"byte":"16385", "to":"TapNumber", "queryOnly":true}
}
```

## Data types class diagram
![This is an image](images/mappingClassDiagram.png)
