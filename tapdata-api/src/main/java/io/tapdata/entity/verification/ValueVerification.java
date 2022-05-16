package io.tapdata.entity.verification;

import java.util.Map;

/**
 * Compare values for source and target
 */
public interface ValueVerification {
    int EQUALS_TYPE_EXACTLY = 1;
    int EQUALS_TYPE_FUZZY = 2;
    /**
     * Compare left and right maps.
     *
     * @param leftMap normally the older map to compare.
     * @param rightMap normally the newer map to compare.
     * @param equalsType compare type, exactly match or fuzzy match.
     * @return return difference information.
     */
    MapDiff mapEquals(Map<String, Object> leftMap, Map<String, Object> rightMap, int equalsType);
}
