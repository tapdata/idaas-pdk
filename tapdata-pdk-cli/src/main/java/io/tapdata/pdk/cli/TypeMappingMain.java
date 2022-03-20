package io.tapdata.pdk.cli;

import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.ExpressionMatchingMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeMappingMain {
    public static void main(String[] args) {
        String str = "{\n" +
                "    \"tinyint[($m)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"smallint[($m)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"mediumint[($m)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"int[($m)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"bigint[($m)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"float[($float)][unsigned][zerofill]\": {\"fbit\": 16, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"double[($float)][unsigned][zerofill]\": {\"float\": 256, \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"decimal($precision, $scale)[unsigned][zerofill]\": {\"precision\":[1, 65], \"scale\": [0, 30], \"unsigned\": \"unsigned\", \"to\": \"typeNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"to\": \"typeDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"to\": \"typeInterval:typeNumber\"},\n" +
                "    \"year[($m)]\": {\"range\": [1901, 2155], \"to\": \"typeYear:typeNumber\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"to\": \"typeDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"typeDateTime\"},\n" +
                "    \"char[($width)]\": {\"byte\": 255, \"to\": \"typeString\"},\n" +
                "    \"varchar[($width)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"typeString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"typeBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"typeString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"typeBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"typeString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"typeBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"typeString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"typeBinary\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"typeString\"},\n" +
                "    \"bit($width)\": {\"byte\": 8, \"to\": \"typeBinary\"},\n" +
                "    \"binary($width)\": {\"byte\": 255, \"to\": \"typeBinary\"},\n" +
                "    \"varbinary($width)\": {\"byte\": 255, \"fixed\": false, \"to\": \"typeBinary\"},\n" +
                "    \"[varbinary]($width)[ABC$hi]aaaa[DDD[AAA]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"typeBinary\"}\n" +
                "}";

//        String str1 = "{\n" +
//                "  \"[tinyint]f[unsigned$ggg][23213$ggg zero$fill]\" : {\"to\" : \"tapString\"}\n" +
//                "}";
        DefaultMap json = InstanceFactory.instance(JsonParser.class).fromJson(str);
        ExpressionMatchingMap matchingMap = new ExpressionMatchingMap(json);
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
        //[varbinary]($width)[ABC$hi] aaaa [DDD[AAA]]
        System.out.println("varbinary(lllll)ABCDDDDD aaaa DDDAAA => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("varbinary(lllll)ABCDDDDD aaaa DDDAAA")));
        System.out.println("(lllll)ABCDDDDD aaaa => " + InstanceFactory.instance(JsonParser.class).toJson(matchingMap.get("(lllll)ABCDDDDD aaaa")));
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
