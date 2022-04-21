package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

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
                "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
                "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +

                "    \"boolean\":{\"bit\":8, \"unsigned\":\"\", \"to\":\"TapNumber\"},\n" +
                "    \"tinyint\":{\"bit\":8, \"to\":\"TapNumber\"},\n" +
                "    \"smallint\":{\"bit\":16, \"to\":\"TapNumber\"},\n" +
                "    \"int\":{\"bit\":32, \"to\":\"TapNumber\"},\n" +
                "    \"bigint\":{\"bit\":64, \"to\":\"TapNumber\"},\n" +
                "    \"largeint\":{\"bit\":128, \"to\":\"TapNumber\"},\n" +
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
//                .add(field("tinytext", "tinytext"))
//                .add(field("datetime", "datetime"))
//                .add(field("bigint", "bigint"))
//                .add(field("bigint unsigned", "bigint unsigned"))
//                .add(field("char(300)", "char(300)"))
//                .add(field("double(32)", "double(32)"))
//                .add(field("mediumtext", "mediumtext"))
//                .add(field("bit(8)", "bit(8)")) //no binary in target types
//                .add(field("binary(200)", "binary(200)"))
//                .add(field("timestamp", "timestamp"))
//                .add(field("mediumint unsigned", "mediumint unsigned"))

        ;
        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
//        assertNotNull(sourceTable.getNameFieldMap().get("tinytext").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("char(300)").getTapType());
//        assertNotNull(sourceTable.getNameFieldMap().get("bit(8)").getTapType());

        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        //源端一个byte等于3个byte， 目标端一个byte等于2个byte的case， 适用于解决byte有时是byte， 有时是char的问题
        //Source: "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"byteRatio\": 3, \"fixed\": false, \"to\": \"TapString\"},\n" +
        //Target: "    \"char[($byte)]\":{\"byte\":255, \"byteRatio\": 2, \"to\": \"TapString\", \"defaultByte\": 1},\n" +
        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();
        TapField varchar10Field = nameFieldMap.get("varchar(10)");
        assertEquals("char(15)", varchar10Field.getOriginType());
        assertEquals(30, ((TapString)varchar10Field.getTapType()).getBytes());

        //源端一个bit等于3个bit， 目标端一个bit等于2个bit的case， 适用于解决bit有时是bit， 有时是byte的问题。
        //Source: "    \"int[($bit)][unsigned][zerofill]\": {\"bit\": 32, \"bitRatio\": 3, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
        //Target: "    \"myint[($bit)][unsigned]\":{\"bit\":48, \"bitRatio\": 2, \"unsigned\":\"unsigned\", \"to\":\"TapNumber\"},\n" +
        TapField int32unsignedField = nameFieldMap.get("int(32) unsigned");
        assertEquals("myint(48) unsigned", int32unsignedField.getOriginType());
        assertEquals(96, ((TapNumber)int32unsignedField.getTapType()).getBit());

        //源端scale是负数， 目标端不支持负数的case
        //Source: "    \"decimal($precision,$scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [-3, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
        //Target: "    \"decimal[($precision,$scale)]\":{\"precision\": [1, 27], \"defaultPrecision\": 10, \"scale\": [0, 9], \"defaultScale\": 0, \"to\": \"TapNumber\"},\n" +
        TapField decimal273 = nameFieldMap.get("decimal(27, -3)");
        assertEquals("decimal(27,0)", decimal273.getOriginType());
        assertEquals(-3, ((TapNumber)decimal273.getTapType()).getScale());

        //源端的类型大于任何目标端的类型， 因此在目标端选择尽可能大的类型
        //Source: "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
        //Target: "    \"string\":{\"byte\":\"2147483643\", \"to\":\"TapString\"},\n" +
        TapField longtext = nameFieldMap.get("longtext");
        assertEquals("string", longtext.getOriginType());
        assertEquals(4294967296L, ((TapString)longtext.getTapType()).getBytes());
    }

}
