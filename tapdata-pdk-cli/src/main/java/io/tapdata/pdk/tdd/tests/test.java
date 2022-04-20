package io.tapdata.pdk.tdd.tests;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import java.math.BigDecimal;
import java.util.*;

public class test {
    public static void main(String... args) {
        int a = 127;
        System.out.println("" + (a % 10));

        System.out.println("" + (a / 10 + ((a % 10) > 0 ? 1 : 0)));
    }


}
