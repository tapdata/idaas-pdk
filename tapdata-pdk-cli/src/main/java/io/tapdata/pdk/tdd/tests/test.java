package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.TypeUtils;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class test {
    public static void main(String... args) {
        BigDecimal bigDecimal = new BigDecimal("-999999999");
        System.out.println("value " + bigDecimal + " precision " + bigDecimal.precision() + " scale " + bigDecimal.scale() + " sign " + bigDecimal.signum());
        DecimalFormat decimalFormat = new DecimalFormat();
        BigDecimal theDecimal = new BigDecimal(2);

        System.out.println(TypeUtils.maxValueForBit(24, true));
        System.out.println(TypeUtils.maxValueForBit(24, false));
        System.out.println(TypeUtils.minValueForBit(24, true));
        System.out.println(TypeUtils.minValueForBit(24, false));
        System.out.println(Math.pow(2,24));

        System.out.println("max " + maxValueForPrecision(30));
        System.out.println("min " + minValueForPrecision(30));

        double v1 = 313.1223423423423423423423423423423423425345;
        double v2 = 123424.12;
        System.out.println("number " + (v1 * 10000000d));
        System.out.println("number " + (v2 * 100000000000000d));
        System.out.println("number " + ((v2 * 100000000000000d) + (v1 * 10000000d)));

        System.out.println("aaa " + BigDecimal.valueOf(100d).multiply(BigDecimal.TEN));
    }

    public static BigDecimal maxValueForPrecision(int maxPrecision) {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < maxPrecision; i++) {
            builder.append("9");
        }
        return new BigDecimal(builder.toString());
    }
    public static BigDecimal minValueForPrecision(int maxPrecision) {
        StringBuilder builder = new StringBuilder("-");
        for(int i = 0; i < maxPrecision; i++) {
            builder.append("9");
        }
        return new BigDecimal(builder.toString());
    }
}
