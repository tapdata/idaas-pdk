package io.tapdata.pdk.tdd.tests;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import java.math.BigDecimal;
import java.util.*;

public class test {
    public static void main(String... args) {
        int batchSize = 1000;
        long time = System.currentTimeMillis();
        for (int j = 0; j < 1000; j++) {
//            List<TapEvent> tapEvents = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                Map<String, Object> map = new HashMap<>();
                for(int m = 0; m < 100; m++) {
                    map.put("k" + m, "v" + m);
                }
//                TapInsertRecordEvent recordEvent = new TapInsertRecordEvent().after(map);
//                tapEvents.add(recordEvent);
            }
        }
        System.out.println("takes " + (System.currentTimeMillis() - time));
    }


}
