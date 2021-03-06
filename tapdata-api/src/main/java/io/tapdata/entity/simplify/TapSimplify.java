package io.tapdata.entity.simplify;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.*;

import java.util.*;

public class TapSimplify {
    private static final TapUtils tapUtils = InstanceFactory.instance(TapUtils.class);
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

    public static void interval(Runnable runnable, int seconds) {
        tapUtils.interval(runnable, seconds);
    }

    public static String toJson(Object obj) {
        return jsonParser.toJson(obj);
    }

    public static DataMap fromJson(String json) {
        return jsonParser.fromJson(json);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return jsonParser.fromJson(json, clazz);
    }

    public static String format(String message, Object... args) {
        return FormatUtils.format(message, args);
    }

    public static TapField field(String name, String originType) {
        return new TapField(name, originType);
    }

    public static TapTable table(String tableName, String id) {
        return new TapTable(tableName, id);
    }

    public static TapTable table(String nameAndId) {
        return new TapTable(nameAndId);
    }

    public static TapString tapString() {
        return new TapString();
    }

    public static TapNumber tapNumber() {
        return new TapNumber();
    }

    public static TapRaw tapRaw() {
        return new TapRaw();
    }

    public static TapArray tapArray() {
        return new TapArray();
    }

    public static TapMap tapMap() {
        return new TapMap();
    }

    public static TapYear tapYear() {
        return new TapYear();
    }

    public static TapDate tapDate() {
        return new TapDate();
    }

    public static TapBoolean tapBoolean() {
        return new TapBoolean();
    }

    public static TapBinary tapBinary() {
        return new TapBinary();
    }

    public static TapTime tapTime() {
        return new TapTime();
    }

    public static TapDateTime tapDateTime() {
        return new TapDateTime();
    }

    public static Entry entry(String key, Object value) {
        return new Entry(key, value);
    }

    public static <T> List<T> list(T... ts) {
        return Arrays.asList(ts);
    }

    public static <T> List<T> list() {
        return new ArrayList<T>();
    }

    public static Map<String, Object> map() {
        return new LinkedHashMap<>();
    }

    public static Map<String, Object> map(Entry... entries) {
        Map<String, Object> map = new LinkedHashMap<>();
        if(entries != null) {
            for(Entry entry : entries) {
                if(entry.getKey() != null && entry.getValue() != null)
                    map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public static TapInsertRecordEvent insertRecordEvent(Map<String, Object> after, TapTable tapTable) {
        return new TapInsertRecordEvent().init().after(after).table(tapTable);
    }

    public static TapDeleteRecordEvent deleteDMLEvent(Map<String, Object> before, TapTable tapTable) {
        return new TapDeleteRecordEvent().init().before(before).table(tapTable);
    }

    public static TapUpdateRecordEvent updateDMLEvent(Map<String, Object> before, Map<String, Object> after, TapTable tapTable) {
        return new TapUpdateRecordEvent().init().before(before).after(after).table(tapTable);
    }

    public static void sleep(long milliseconds) {
        if(milliseconds < 0)
            return;
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
    }

    public static Object convertDateTimeToDate(DateTime dateTime) {
        if(dateTime != null) {
            Long milliseconds;
            Long nano = dateTime.getNano();
            Long seconds = dateTime.getSeconds();
            if(nano != null) {
                milliseconds = nano / 1000 / 1000;
            } else if(seconds != null) {
                milliseconds = seconds * 1000;
            } else {
                return null;
            }
            return new Date(milliseconds);
        }
        return null;
    }
}
