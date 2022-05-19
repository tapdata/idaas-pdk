package io.tapdata.connector.postgres.kit;

import java.util.Collection;
import java.util.Map;

/**
 * @author Jarad
 * @date 2022/4/29
 */
public class EmptyKit {

    public static boolean isEmpty(Collection<?> var) {
        return null == var || var.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> var) {
        return null == var || var.isEmpty();
    }

    public static boolean isEmpty(String var) {
        return null == var || var.isEmpty();
    }

    public static boolean isNotEmpty(Collection<?> var) {
        return null != var && !var.isEmpty();
    }

    public static boolean isNotEmpty(Map<?, ?> var) {
        return null != var && !var.isEmpty();
    }

    public static boolean isNotEmpty(String var) {
        return null != var && !var.isEmpty();
    }

    public static boolean isNull(Object var) {
        return null == var;
    }

    public static boolean isNotNull(Object var) {
        return null != var;
    }

}
