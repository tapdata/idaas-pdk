package io.tapdata.pdk.cli;

import io.tapdata.entity.mapping.*;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeMappingMain {
    public static void main(String[] args) {
        String str = "{\n" +
                "    \"tinyint[($length)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($length)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($length)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"int[($length)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($length)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($length)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($length)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision, $scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [0, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($width)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"varchar[($width)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"bit($width)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($width)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($width)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($width)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";

//        String str1 = "{\n" +
//                "  \"[tinyint]f[unsigned$ggg][23213$ggg zero$fill]\" : {\"to\" : \"tapString\"}\n" +
//                "}";

//        ExpressionMatchingMap<TapMapping> map1 = ExpressionMatchingMap.map(str, new TypeHolder<Map<String, TapMapping>>(){});
//        map1.get("");
        DefaultExpressionMatchingMap matchingMap = ExpressionMatchingMap.map(str);
        matchingMap.setValueFilter(defaultMap -> {
            TapMapping tapMapping = (TapMapping) defaultMap.get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping == null) {
                defaultMap.put(TapMapping.FIELD_TYPE_MAPPING, TapMapping.build(defaultMap));
            }
        });

//        TapMapping tapMapping = TapMapping.build(matchingMap.get("binary(4)").getValue());
//        TapMapping tapMapping1 = TapMapping.build(matchingMap.get("double").getValue());
//        TapMapping tapMapping2 = TapMapping.build(matchingMap.get("longtext").getValue());
//        TapMapping tapMapping3 = TapMapping.build(matchingMap.get("text").getValue());
//        TapMapping tapMapping4 = TapMapping.build(matchingMap.get("date").getValue());
//        TapMapping tapMapping5 = TapMapping.build(matchingMap.get("year(1)").getValue());
//        TapMapping tapMapping6 = TapMapping.build(matchingMap.get("time").getValue());
//        TapMapping tapMapping7 = TapMapping.build(matchingMap.get("decimal(34, 3) unsigned zerofill").getValue());
//        TapMapping tapMapping8 = TapMapping.build(matchingMap.get("datetime").getValue());

//        DefaultMap defaultMap = InstanceFactory.instance(JsonParser.class).fromJson(str, DefaultMap.class);
//        DefaultExpressionMatchingMap map2 = ExpressionMatchingMap.map(defaultMap);
//        map2.get("");

        System.out.println("Binary(4) => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("Binary(4)")));
        System.out.println("double => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("double")));
        System.out.println("double(4) unsigned zerofill => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("double(4) unsigned zerofill")));
        //decimal($precision, $scale)[unsigned][zerofill]
        System.out.println("decimal(4, 8) unsigned zerofill => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("decimal(4, 8) unsigned zerofill")));
        System.out.println("decimal(8, 3) => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("decimal(8, 3)")));
        System.out.println("decimal(56, 2) unsigned => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("decimal(56, 2) unsigned")));
        //varchar[($width)]
        System.out.println("varchar => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("varchar")));
        System.out.println("varchar(4) => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("varchar(4)")));
        //date
        System.out.println("date => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("date")));
        //[varbinary]($width)[ABC$hi] aaaa [DDD[AAA]]
        System.out.println("varbinary(lllll)ABCDDDDD aaaa DDDAAA => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("varbinary(lllll)ABCDDDDD aaaa DDDAAA")));
        System.out.println("varbinary(lllll)ABCDDDDD aaaa DDDBBB => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("varbinary(lllll)ABCDDDDD aaaa DDDBBB")));
        System.out.println("(lllll)ABCDDDDD aaaa => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("(lllll)ABCDDDDD aaaa")));

        TypeExprResult<DefaultMap> result = matchingMap.get("decimal(56, 2) theUnsigned theZerofill");
        TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
        TapType tapType = tapMapping.toTapType("decimal(56, 2) theUnsigned theZerofill", result.getParams());

        String originType = tapMapping.fromTapType("decimal($precision, $scale)[theUnsigned][theZerofill]", tapType);

        matchingMap.iterate(entry -> {
            System.out.println("key " + entry.getKey() + " value " + entry.getValue());
            return false;
        });

        System.out.println("");
//        long time = System.currentTimeMillis();
//        for(int i = 0; i < 1000000; i++) {
//            matchingMap.get("decimal(4, 8) unsigned zerofill");
//        }
//        System.out.println("matchingMap.get(\"decimal(4, 8) unsigned zerofill\") takes " + (System.currentTimeMillis() - time));
        //1000000 matchingMap.get("decimal(4, 8) unsigned zerofill") takes 3408
    }

    private static String a(String str) {
        String[] strs = str.split("]");
        String ggg = "abc";
        String fill = "ffff";
        String s = "[tinyint]f[unsigned$ggg][23213$gggzero$fill]";
        String s1 = "tinyintfunsigned$abc23213abczeroffff";
        String s2 = "funsignedabc23213abczeroffff";
        String s3 = "f";
        String s4 = "f23213abczeroffff";

        String regex = "(tinyint|)f(unsigned.*|)(23213.*zero.*|)$";
        Pattern r = Pattern.compile(regex);
        Matcher m = r.matcher(s2);
        System.out.println(m.matches());
        return "";
    }

    private static String b(String str) {
        String[] strs = str.split("]");
        String ggg = "abc";
        String fill = "ffff";
        String s = "tinyint[($m)][unsigned][zerofill]";
        String s1 = "tinyintfunsigned$abc23213abczeroffff";
        String s2 = "tinyint(4) unsigned";
        String s3 = "f";
        String s4 = "f23213abczeroffff";

        String regex = "tinyint(\\((.*)\\)|)(\\s|)(unsigned|)(\\s|)(zerofill|)";
        Pattern r = Pattern.compile(regex);
        Matcher m = r.matcher(s2);
        String title = "";
        MatchResult matchResult = m.toMatchResult();

        return "";
    }
}
