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

    public static Long objectToNumber(Object obj) {
        Long number = null;
        if(obj instanceof Number) {
            number = ((Number) obj).longValue();
        } else if(obj instanceof String) {
            //4g 64k 16m
            String str = (String) obj;
            str = str.trim().toLowerCase();
            if(str.endsWith("k")) {
                number = calculateBytes(str, 1024);
            } else if(str.endsWith("m")) {
                number = calculateBytes(str, 1024L * 1024);
            } else if(str.endsWith("g")) {
                number = calculateBytes(str, 1024L * 1024 * 1024);
            } else if(str.endsWith("t")) {
                number = calculateBytes(str, 1024L * 1024 * 1024 * 1024);
            } else if(str.endsWith("p")) {
                number = calculateBytes(str, 1024L * 1024 * 1024 * 1024 * 1024);
            } else {
                try {
                    number = Long.parseLong(str);
                } catch(Throwable ignored) {}
            }
        }
        return number;
    }

    private static Long calculateBytes(String str, long ratio) {
        String numberStr = str.substring(0, str.length() - 1);
        try {
            long num = Long.parseLong(numberStr);
            return num * ratio - 1;
        } catch(Throwable ignored) {}
        return null;
    }
}
