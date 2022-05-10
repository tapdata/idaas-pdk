# Data Types

Data types is the mapping of data type (include capabilities) with TapType.   
TapType is the generic type definition in iDaaS Incremental Engine.

* PDK connector define data types json mapping, let Incremental Engine know the mapping of data types with TapTypes.
* Records with TapTypes will flow into Incremental Engine to do processing, join, etc.
* Once the records with TapTypes will flow into a PDK target, Incremental engine will conjecture the best data types for PDK developer to create the table, base on the input of data types json mapping.

For more about [Data Types](dataTypes.md).

If without TapType, the conversion lines is like below, which is very complex to maintain.
![This is an image](images/withoutTapType.png)

With TapType in the middle of type conversion, the conversion can be maintainable and the fundamental for data processing, join, etc.

![This is an image](images/withTapType.png)

Above is the important concept to implement PDK connector, especially for the data source which need create table for insert records.

### TapType
There are 11 types of TapType.
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

We also have 11 types of TapValue for each TapType to combine value with it's TapType.
* TapBooleanValue
* TapDateValue
* TapArrayValue
* TapRawValue
* TapNumberValue
* TapBinaryValue
* TapTimeValue
* TapMapValue
* TapStringValue
* TapDateTimeValue
* TapYearValue

Below is schema class diagram
![This is an image](images/schemaClassDiagram.png)


Data Types in spec.json is to describe the capabilities of data types for Incremental Engine to conjecture the best data types to create table before record insertion. 

**If the database support insert record without table creation, like MongoDB, Kafka, etc, then please ignore this document, just leave dataTypes empty in spec.json.**

## Data type expression

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

Common fields
```text
{
  "queryOnly" : true, //Optional, default is false. The type is only for query, will not be used for table creation. 
  "priority" : 1 //Optional, default is Integer.MAX_VALUE. If source type matches multiple target types which is the same score (bit or bytes), then the target type will be selected when the priority is the smallest.
  "pkEnablement" : true //Optional, default is true. Whether the data type can be primary key or not. 
}
```

Some TapType have extra fields
* TapNumber
```text
{ 
    "to": "TapNumber",
  
    "bit": 64, //max bit
    "defaultBit" : 10, //default bit
    "bitRatio" : 1, //1 is default. Some databases, bit means bit, but some bit means byte, then "bitRatio" should be 8. 
    
    "precision" : [1, 30], //precision range, [min, max]
    "precisionDefault" : 10, //default precision
    
    "scale" : [0, 30], //scale range, [min, max], distinguish int, long and float, double.  
    "scaleDefault" : 3, //default scale
    
    "value": [-128, 127], //Also support like [ "-3.402823466E+38", "3.402823466E+38"], the min/max value of this data type.
    "unsignedValue": [0, 255], //Also support like [ "-3.402823466E+38", "3.402823466E+38"], the min/max value of this data type when unsigned. 
    
    "fixed" : false, // Default is false, if true, means it is important to preserve exact precision, normally with precision and scale specified. Otherwise only precision or none specified.  
    
    "unsigned" : "unsignedEx", //alias of unsigned, different database may use different label for unsigned
    "zerofill" : "zerofill" //alias of zerofill, different database may use different label for zerofill
}
```
For the data type accuracy, we prefer

**value/unsignedValue > bit > precision**


for example

```text
{
    "tinyint[unsigned]": {"to": "TapNumber","bit": 8,"precision": 3,"value": [ -128, 127],"unsignedValue": [ 0, 255],"unsigned": "unsigned"},
    "smallint[unsigned]": {"to": "TapNumber","bit": 16,"value": [ -32768, 32767],"unsignedValue": [ 0, 65535],"unsigned": "unsigned","precision": 5},
    "mediumint[unsigned]": {"to": "TapNumber","bit": 24,"precision": 7,"value": [ -8388608, 8388607],"unsignedValue": [ 0, 16777215],"unsigned": "unsigned"},
    "int[unsigned]": {"to": "TapNumber","bit": 32,"precision": 10,"value": [ -2147483648, 2147483647],"unsignedValue": [ 0, 4294967295],"unsigned": "unsigned"},
    "bigint[unsigned]": {"to": "TapNumber","bit": 64,"precision": 19,"value": [ -9223372036854775808, 9223372036854775807], "unsignedValue": [ 0, 18446744073709551615],"unsigned": "unsigned"},
    "decimal[($precision,$scale)][unsigned]": {"to": "TapNumber","precision": [ 1, 65],"scale": [ 0, 30],"defaultPrecision": 10,"defaultScale": 0,"unsigned": "unsigned", "fixed": true},
    "float($precision,$scale)[unsigned]": {"to": "TapNumber","precision": [ 1, 30],"scale": [ 0, 30],"value": [ "-3.402823466E+38", "3.402823466E+38"],"unsigned": "unsigned","fixed": false},
    "float": {"to": "TapNumber","precision": [ 1, 6],"scale": [ 0, 6],"fixed": false},
    "double": {"to": "TapNumber","precision": [ 1, 11],"scale": [ 0, 11],"fixed": false},
    "double[($precision,$scale)][unsigned]": {"to": "TapNumber","precision": [ 1, 255],"scale": [ 0, 30],"value": [ "-1.7976931348623157E+308", "1.7976931348623157E+308"],"unsigned": "unsigned","fixed": false}
}

```    
* TapString
```text
{
    "byte" : "16m", //Max length of string, support string with suffix "k", "m", "g", "t", "p", also number
    "defaultByte" : "1m", //Max length of string, support string with suffix "k", "m", "g", "t", "p", also number
    "byteRatio" : 1, // 1 is default. Some databases, byte means byte, but some byte means char, like utf8 1 char mean 3 bytes, then "bitRatio" should be 3. 
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
}
```
* TapDateTime
```text
{
    "range" : ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], //Date time range in format YYYY-MM-DD hh:mm:ss
}
```
* TapTime
```text
{
    "range" : ["-838:59:59","838:59:59"], //Time range in format hh:mm:ss
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
