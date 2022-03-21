package io.tapdata.pdk.cli;

import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.mapping.ExpressionMatchingMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeMappingMain {
    public static void main(String[] args) {
        String str = "{\n" +
                "    \"tinyint[($m)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($m)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($m)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"int[($m)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"bigint[($m)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"float[($float)][unsigned][zerofill]\": {\"fbit\": 16, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"double[($float)][unsigned][zerofill]\": {\"float\": 256, \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision, $scale)[unsigned][zerofill]\": {\"precision\":[1, 65], \"scale\": [0, 30], \"unsigned\": true, \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"to\": \"TapInterval:typeNumber\"},\n" +
                "    \"year[($m)]\": {\"range\": [1901, 2155], \"to\": \"TapYear:typeNumber\"},\n" +
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
//                "    \"[varbinary]($width)[ABC$hi]aaaa[DDD[AAA]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";

//        String str1 = "{\n" +
//                "  \"[tinyint]f[unsigned$ggg][23213$ggg zero$fill]\" : {\"to\" : \"tapString\"}\n" +
//                "}";

//        ExpressionMatchingMap<TapMapping> map1 = ExpressionMatchingMap.map(str, new TypeHolder<Map<String, TapMapping>>(){});
//        map1.get("");
        ExpressionMatchingMap<DefaultMap> matchingMap = ExpressionMatchingMap.map(str);
//        DefaultMap defaultMap = InstanceFactory.instance(JsonParser.class).fromJson(str, DefaultMap.class);
//        DefaultExpressionMatchingMap map2 = ExpressionMatchingMap.map(defaultMap);
//        map2.get("");

        System.out.println("binary(4) => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("binary(4)")));
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
        System.out.println("(lllll)ABCDDDDD aaaa => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("(lllll)ABCDDDDD aaaa")));



        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            matchingMap.get("decimal(4, 8) unsigned zerofill");
        }
        System.out.println("matchingMap.get(\"decimal(4, 8) unsigned zerofill\") takes " + (System.currentTimeMillis() - time));
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
