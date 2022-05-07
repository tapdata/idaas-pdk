package io.tapdata.entity.utils;

import java.math.BigDecimal;

public class TypeUtils {
    private static final BigDecimal TWO = new BigDecimal(2);
    /**
     * this is not accurate way to find out max value. Only be used when no max value or bit specified.
     *
     * @param maxPrecision
     * @return
     */
    public static BigDecimal maxValueForPrecision(int maxPrecision) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < maxPrecision; i++) {
            builder.append("9");
        }
        return new BigDecimal(builder.toString());
    }

    /**
     * this is not accurate way to find out min value. Only be used when no max value or bit specified.
     *
     * @param maxPrecision
     * @return
     */
    public static BigDecimal minValueForPrecision(int maxPrecision) {
        StringBuilder builder = new StringBuilder("-");
        for(int i = 0; i < maxPrecision; i++) {
            builder.append("9");
        }
        return new BigDecimal(builder.toString());
    }

    public static BigDecimal maxValueForBit(int bit, Boolean unsigned) {
        if(unsigned != null && unsigned) {
            return TWO.pow(bit).subtract(BigDecimal.ONE);
        } else {
            return TWO.pow(bit - 1).subtract(BigDecimal.ONE);
        }
    }

    public static BigDecimal minValueForBit(int bit, Boolean unsigned) {
        if(unsigned != null && unsigned) {
            return BigDecimal.ZERO;
        } else {
            return TWO.pow(bit - 1).negate();
        }
    }
}
