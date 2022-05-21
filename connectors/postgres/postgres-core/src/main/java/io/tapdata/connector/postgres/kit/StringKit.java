package io.tapdata.connector.postgres.kit;

import java.util.Collection;

public class StringKit {

    public final static String STRING_EMPTY = "";
    public final static String STRING_COMMA = ",";
//    public final static String STRING_SPACE = " ";

    public static String combineString(Collection<String> strings, String combiner) {
        if (EmptyKit.isEmpty(strings)) {
            return STRING_EMPTY;
        }
        if (null == combiner) {
            return combineString(strings, STRING_EMPTY);
        }
        return strings.stream().reduce((v1, v2) -> v1 + combiner + v2).orElseGet(String::new);
    }

    public static String combineStringWithComma(Collection<String> strings) {
        return combineString(strings, STRING_COMMA);
    }

}
