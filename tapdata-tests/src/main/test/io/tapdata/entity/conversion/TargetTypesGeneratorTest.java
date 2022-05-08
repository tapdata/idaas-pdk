package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.*;

class TargetTypesGeneratorTest {
    private TargetTypesGenerator targetTypesGenerator;
    private TableFieldTypesGenerator tableFieldTypesGenerator;
    private TapCodecFilterManager targetCodecFilterManager;
    private TapCodecRegistry codecRegistry;

    @BeforeEach
    void beforeEach() {
        targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        if(targetTypesGenerator == null)
            throw new CoreException(ErrorCodes.SOURCE_TARGET_TYPES_GENERATOR_NOT_FOUND, "TargetTypesGenerator's implementation is not found in current classloader");
        tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        if(tableFieldTypesGenerator == null)
            throw new CoreException(ErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");
        codecRegistry = TapCodecRegistry.create();
        targetCodecFilterManager = TapCodecFilterManager.create(codecRegistry);
    }
    /*
    @Test
    void convert() {
        String sourceTypeExpression = "{\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"gmt\": 8, \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{\n" +
//                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
//                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
//                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
//                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
//                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
//                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
//                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
//                "    \"myint[($bit)][unsigned]\":{\"bit\":32, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
//                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
//                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
//                "    \"date\":{\"byte\":3, \"range\":[\"0000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
//                "    \"datetime\":{\"byte\":8, \"range\":[\"0000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"to\":\"TapDateTime\"},\n" +
//                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
//                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
//                .add(field("tinytext", "tinytext"))
//                .add(field("datetime", "datetime"))
//                .add(field("bigint", "bigint"))
//                .add(field("bigint unsigned", "bigint unsigned"))
//                .add(field("bigint(32) unsigned", "bigint(32) unsigned"))
//                .add(field("char(300)", "char(300)"))
//                .add(field("decimal(27, -3)", "decimal(27, -3)"))
//                .add(field("longtext", "longtext")) // exceed the max of target types
//                .add(field("double(32)", "double(32)"))
//                .add(field("mediumtext", "mediumtext"))
                .add(field("bit(8)", "bit(8)")) //no binary in target types
//                .add(field("binary(200)", "binary(200)"))
//                .add(field("varchar(10)", "varchar(10)"))
//                .add(field("timestamp", "timestamp"))
//                .add(field("mediumint unsigned", "mediumint unsigned"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        assertNotNull(sourceTable.getNameFieldMap().get("tinytext").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("char(300)").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("bit(8)").getTapType());

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

    }*/

    @Test
    void convertTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +

                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"gmt\": 8, \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{\n" +
                "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +

                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"float\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"double\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"date\":{\"byte\":3, \"range\":[\"0000-01-01\", \"9999-12-31\"], \"to\":\"TapDate\"},\n" +
                "    \"datetime\":{\"byte\":8, \"range\":[\"0000-01-01 00:00:00\",\"9999-12-31 23:59:59\"],\"to\":\"TapDateTime\"},\n" +
                "    \"varchar[($byte)]\":{\"byte\":\"65535\", \"to\":\"TapString\"},\n" +
                "    \"HLL\":{\"byte\":\"16385\", \"to\":\"TapNumber\", \"queryOnly\":true}\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int(32) unsigned", "int(32) unsigned"))
                .add(field("longtext", "longtext")) // exceed the max of target types
                .add(field("varchar(10)", "varchar(10)"))
                .add(field("decimal(27, -3)", "decimal(27, -3)"))


        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        assertNotNull(sourceTable.getNameFieldMap().get("tinytext").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("char(300)").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("bit(8)").getTapType());

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        //源端一个bit等于3个bit， 目标端一个bit等于2个bit的case， 适用于解决bit有时是bit， 有时是byte的问题。
        //Source: "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
        //Target: "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
        TapField int32unsignedField = nameFieldMap.get("int(32) unsigned");
        assertEquals("myint(48) unsigned", int32unsignedField.getDataType());
        assertEquals(96, ((TapNumber)int32unsignedField.getTapType()).getBit());

        //源端一个byte等于3个byte， 目标端一个byte等于2个byte的case， 适用于解决byte有时是byte， 有时是char的问题
        //Source: "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
        //Target: "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
        TapField varchar10Field = nameFieldMap.get("varchar(10)");
        assertEquals("char(15)", varchar10Field.getDataType());
        assertEquals(30, ((TapString)varchar10Field.getTapType()).getBytes());

        //源端scale是负数， 目标端不支持负数的case
        //Source: "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
        //Target: "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
        TapField decimal273 = nameFieldMap.get("decimal(27, -3)");
        assertEquals("decimal(27,0)", decimal273.getDataType());
        assertEquals(-3, ((TapNumber)decimal273.getTapType()).getScale());

        //源端的类型大于任何目标端的类型， 因此在目标端选择尽可能大的类型
        //Source: "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
        //Target: "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
        TapField longtext = nameFieldMap.get("longtext");
        assertEquals("string", longtext.getDataType());
        assertEquals(4294967295L, ((TapString)longtext.getTapType()).getBytes());
    }

    @Test
    void convertPriorityTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "}";
        String targetTypeExpression = "{\n" +
                "    \"myint[($bit)][unsigned]\":{\"bit\":32, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":32, \"priority\":1, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":32, \"priority\":3, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"priority\":2, \"to\":\"TapNumber\"},\n" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int(32)", "int(32)"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField int32unsignedField = nameFieldMap.get("int(32)");
        assertEquals("tinyint", int32unsignedField.getDataType());
    }


    @Test
    void numberTest() {
        String sourceTypeExpression = "{\n" +
                "    \"int[($bit)][unsigned]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[unsigned]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"unsigned\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"fixed\": true, \"to\": \"TapNumber\"},\n" +

                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"scale\": [ 0, 6], \"fixed\": false, \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"scale\": [ 0, 6], \"fixed\": false, \"to\": \"TapNumber\"},\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ 0, 4294967295], \"unsigned\": \"unsigned\"},\n" +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
                "\"superbigint\": {\"to\": \"TapNumber\",\"bit\": 640},\n" +
                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\"},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false}" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("int unsigned", "int unsigned"))
                .add(field("int(32)", "int(32)"))
                .add(field("decimal(65,30) unsigned", "decimal(65,30) unsigned"))
                .add(field("decimal(65,-3)", "decimal(65,-3)"))
                .add(field("decimal(65,30)", "decimal(65,30)"))
                .add(field("float", "float"))
                .add(field("float unsigned", "float unsigned"))
                .add(field("float(8)", "float(8)"))
                .add(field("double(256) unsigned", "double(256) unsigned"))
                .add(field("double", "double"))
                .add(field("bigint(150)", "bigint(150)"))
                .add(field("bigint(50)", "bigint(50)"))
        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField int32unsignedField = nameFieldMap.get("int(32)");
        assertEquals("int", int32unsignedField.getDataType());

        TapField decimal650Field = nameFieldMap.get("decimal(65,-3)");
        assertEquals("decimal(65,0)", decimal650Field.getDataType());

        TapField decimal6530Field = nameFieldMap.get("decimal(65,30)");
        assertEquals("decimal(65,30)", decimal6530Field.getDataType());

        TapField floatField = nameFieldMap.get("float");
        assertEquals("float", floatField.getDataType());

        TapField floatUnsignedField = nameFieldMap.get("float unsigned");
        assertEquals("float(5,6) unsigned", floatUnsignedField.getDataType());

        TapField float8Field = nameFieldMap.get("float(8)");
        assertEquals("float", float8Field.getDataType());

        TapField double256UnsignedField = nameFieldMap.get("double(256) unsigned");
        assertEquals("double(77,6) unsigned", double256UnsignedField.getDataType());

        TapField doubleField = nameFieldMap.get("double");
        assertEquals("double(77,6)", doubleField.getDataType());

        TapField bigint150Field = nameFieldMap.get("bigint(150)");
        assertEquals("superbigint", bigint150Field.getDataType());

        TapField bigint50Field = nameFieldMap.get("bigint(50)");
        assertEquals("bigint", bigint50Field.getDataType());
    }

    @Test
    void stringTest() {
                String sourceTypeExpression = "{\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"superlongtext\": {\"byte\": \"8g\", \"to\": \"TapString\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\", \"byteRatio\": 3, \"fixed\": true, },\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +

                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"gmt\": 8, \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +

                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ 0, 4294967295]},\n" +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"format\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [ \"-838:59:59\", \"838:59:59\"]},\n" +
                "\"datetime[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0},\n" +
                "\"timestamp[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0,\"withTimezone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("varchar(400)", "varchar(400)"))
                .add(field("varchar(40)", "varchar(40)"))
                .add(field("char(30)", "char(30)"))
                .add(field("tinytext", "tinytext"))
                .add(field("text", "text"))
                .add(field("longtext", "longtext"))
                .add(field("superlongtext", "superlongtext"))
                .add(field("varchar(64k)", "varchar(64k)"))
                .add(field("varchar", "varchar"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField varchar400Field = nameFieldMap.get("varchar(400)");
        assertEquals("varchar(400)", varchar400Field.getDataType());

        TapField varchar40Field = nameFieldMap.get("varchar(40)");
        assertEquals("varchar(40)", varchar40Field.getDataType());

        TapField char30Field = nameFieldMap.get("char(30)");
        assertEquals("char(30)", char30Field.getDataType());

        TapField tinytextField = nameFieldMap.get("tinytext");
        assertEquals("varchar(255)", tinytextField.getDataType());

        TapField textField = nameFieldMap.get("text");
        assertEquals("varchar(65535)", textField.getDataType());

        TapField longtextField = nameFieldMap.get("longtext");
        assertEquals("longtext", longtextField.getDataType());

        TapField superlongtextField = nameFieldMap.get("superlongtext");
        assertEquals("longtext", superlongtextField.getDataType());

        TapField varchar64kField = nameFieldMap.get("varchar(64k)");
        assertEquals("varchar(65535)", varchar64kField.getDataType());

        TapField varcharField = nameFieldMap.get("varchar");
        assertEquals("varchar(1)", varcharField.getDataType());
    }

    @Test
    void binaryTest() {
        String sourceTypeExpression = "{\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"superlongtext\": {\"byte\": \"8g\", \"to\": \"TapString\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\", \"byteRatio\": 3, \"fixed\": true, },\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +

                "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"tinyint[($bit)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($bit)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($bit)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($bit)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($bit)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($bit)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"gmt\": 8, \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"gmt\": 8, \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"gmt\": 8, \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +

                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";
        String targetTypeExpression = "{" +
                "\"char[($byte)]\": {\"to\": \"TapString\",\"byte\": 255, \"byteRatio\": 3, \"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varchar($byte)\": {\"to\": \"TapString\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinytext\": {\"to\": \"TapString\",\"byte\": 255},\n" +
                "\"text\": {\"to\": \"TapString\",\"byte\": \"64k\"},\n" +
                "\"mediumtext\": {\"to\": \"TapString\",\"byte\": \"16m\"},\n" +
                "\"longtext\": {\"to\": \"TapString\",\"byte\": \"4g\"},\n" +
                "\"json\": {\"to\": \"TapMap\",\"byte\": \"4g\",\"queryOnly\": true},\n" +
                "\"binary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 255,\"defaultByte\": 1,\"fixed\": true},\n" +
                "\"varbinary[($byte)]\": {\"to\": \"TapBinary\",\"byte\": 65535,\"defaultByte\": 1},\n" +
                "\"tinyblob\": {\"to\": \"TapBinary\",\"byte\": 255},\n" +
                "\"blob\": {\"to\": \"TapBinary\",\"byte\": \"64k\"},\n" +
                "\"mediumblob\": {\"to\": \"TapBinary\",\"byte\": \"16m\"},\n" +
                "\"longblob\": {\"to\": \"TapBinary\",\"byte\": \"4g\"},\n" +
                "\"bit[($bit)]\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709552000]},\n" +
                "\"tinyint\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ 0, 255]},\n" +
                "\"tinyint unsigned\": {\"to\": \"TapNumber\",\"bit\": 8,\"precision\": 3,\"value\": [ -128, 127],\"unsigned\": \"unsigned\"},\n" +
                "\"smallint\": {\"to\": \"TapNumber\",\"bit\": 16,\"value\": [ -32768, 32767],\"precision\": 5},\n" +
                "\"smallint unsigned\": {\"to\": \"TapNumber\",\"bit\": 16,\"precision\": 5,\"value\": [ 0, 65535],\"unsigned\": \"unsigned\"},\n" +
                "\"mediumint\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 7,\"value\": [ -8388608, 8388607]},\n" +
                "\"mediumint unsigned\": {\"to\": \"TapNumber\",\"bit\": 24,\"precision\": 8,\"value\": [ 0, 16777215],\"unsigned\": \"unsigned\"},\n" +
                "\"int\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ -2147483648, 2147483647]},\n" +
                "\"int unsigned\": {\"to\": \"TapNumber\",\"bit\": 32,\"precision\": 10,\"value\": [ 0, 4294967295]},\n" +
                "\"bigint\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 19,\"value\": [ -9223372036854775808, 9223372036854775807]},\n" +
                "\"bigint unsigned\": {\"to\": \"TapNumber\",\"bit\": 64,\"precision\": 20,\"value\": [ 0, 18446744073709551615], \"unsigned\": \"unsigned\"},\n" +
                "\"decimal[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 65],\"scale\": [ 0, 30],\"defaultPrecision\": 10,\"defaultScale\": 0,\"unsigned\": \"unsigned\", \"fixed\": true},\n" +
                "\"float($precision,$scale)[unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 30],\"scale\": [ 0, 30],\"value\": [ \"-3.402823466E+38\", \"3.402823466E+38\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"float\": {\"to\": \"TapNumber\",\"precision\": [ 1, 6],\"scale\": [ 0, 6],\"fixed\": false},\n" +
                "\"double\": {\"to\": \"TapNumber\",\"precision\": [ 1, 11],\"scale\": [ 0, 11],\"fixed\": false},\n" +
                "\"double[($precision,$scale)][unsigned]\": {\"to\": \"TapNumber\",\"precision\": [ 1, 255],\"scale\": [ 0, 30],\"value\": [ \"-1.7976931348623157E+308\", \"1.7976931348623157E+308\"],\"unsigned\": \"unsigned\",\"fixed\": false},\n" +
                "\"date\": {\"to\": \"TapDate\",\"range\": [ \"1000-01-01\", \"9999-12-31\"],\"format\": \"yyyy-MM-dd\"},\n" +
                "\"time\": {\"to\": \"TapTime\",\"range\": [ \"-838:59:59\", \"838:59:59\"]},\n" +
                "\"datetime[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1000-01-01 00:00:00.000000\", \"9999-12-31 23:59:59.999999\"],\"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0},\n" +
                "\"timestamp[($precision)]\": {\"to\": \"TapDateTime\",\"range\": [ \"1970-01-01 00:00:01.000000\", \"2038-01-19 03:14:07.999999\"],\"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"precision\": [ 0, 6],\"defaultPrecision\": 0,\"withTimezone\": true}\n"
                + "}";

        TapTable sourceTable = table("test");
        sourceTable
//        "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
//                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
//                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
//                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
//                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
//                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
//                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                .add(field("longblob", "longblob"))
                .add(field("tinyblob", "tinyblob"))
                .add(field("blob", "blob"))
                .add(field("mediumblob", "mediumblob"))
                .add(field("bit(8)", "bit(8)"))
                .add(field("varbinary(200)", "varbinary(200)"))
                .add(field("binary(100)", "binary(100)"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        TapField longblobField = nameFieldMap.get("longblob");
        assertEquals("longblob", longblobField.getDataType());

        TapField tinyblobField = nameFieldMap.get("tinyblob");
        assertEquals("varbinary(255)", tinyblobField.getDataType());

        TapField blobField = nameFieldMap.get("blob");
        assertEquals("varbinary(65535)", blobField.getDataType());

        TapField mediumblobField = nameFieldMap.get("mediumblob");
        assertEquals("mediumblob", mediumblobField.getDataType());

        TapField bit8Field = nameFieldMap.get("bit(8)");
        assertEquals("varbinary(8)", bit8Field.getDataType());

        TapField varbinary200Field = nameFieldMap.get("varbinary(200)");
        assertEquals("varbinary(200)", varbinary200Field.getDataType());

        TapField binary100Field = nameFieldMap.get("binary(100)");
        assertEquals("varbinary(100)", binary100Field.getDataType());

    }

}
