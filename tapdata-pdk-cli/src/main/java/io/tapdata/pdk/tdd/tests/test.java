package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import java.math.BigDecimal;
import java.util.*;

public class test {
    public static void main(String... args) {
        double d1 = (((double)(Long.MAX_VALUE - 255)) / Long.MAX_VALUE);
        double d2 = (((double)(Long.MAX_VALUE - 1354)) / Long.MAX_VALUE);
        System.out.println("aaa " + d1);
        System.out.println("aaa " + d2);
        System.out.println("== " + (d2 == d1));
        System.out.println("> " + (d2 > d1));
        System.out.println("< " + (d2 < d1));

        TreeMap<Integer, String> map = new TreeMap<>();
        System.out.println("first " + map.firstEntry());
        map.put(3, "aa");
        map.put(1, "aab");
        map.put(4, "aaa");
        map.put(1, "abbb");
        System.out.println("array " + Arrays.toString(map.values().toArray(new String[0])));
        System.out.println("first " + map.firstEntry().getValue());
    }


}
