package io.tapdata.pdk.tdd.tests;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import java.math.BigDecimal;
import java.util.*;

public class test {
    public static void main(String... args) {
        TimeZone zone = TimeZone.getDefault();
        String zoneStr = JSON.toJSONString(zone);
        TimeZone newZone = JSON.parseObject(zoneStr, TimeZone.class);
    }


}
