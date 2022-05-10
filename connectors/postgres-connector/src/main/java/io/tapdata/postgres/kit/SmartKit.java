package io.tapdata.postgres.kit;

import java.util.Collection;
import java.util.Map;

/**
 * @author Jarad
 * @date 2022/4/29
 */
public class SmartKit {

    public final static String STRING_EMPTY = "";
    public final static String STRING_COMMA = ",";
//    public final static String STRING_SPACE = " ";

    public static String combineString(Collection<String> strings, String combiner) {
        if (isEmpty(strings)) {
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

    public static boolean isEmpty(Collection<?> var) {
        return var == null || var.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> var) {
        return var == null || var.isEmpty();
    }

    public static boolean isNotEmpty(Collection<?> var) {
        return var != null && !var.isEmpty();
    }

    public static boolean isNotEmpty(Map<?, ?> var) {
        return var != null && !var.isEmpty();
    }
}
