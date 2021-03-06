package io.tapdata.base;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.Entry;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.pdk.apis.utils.TypeConverter;

import java.text.SimpleDateFormat;
import java.util.*;

public abstract class ConnectorBase {
    private static final TypeConverter typeConverter = InstanceFactory.instance(TypeConverter.class);
    private static final SimpleDateFormat tapDateTimeFormat = new SimpleDateFormat();

    public static void interval(Runnable runnable, int seconds) {
        TapSimplify.interval(runnable, seconds);
    }

    public static Long toLong(Object value) {
        return typeConverter.toLong(value);
    }

    public static Integer toInteger(Object value) {
        return typeConverter.toInteger(value);
    }

    public static Short toShort(Object value) {
        return typeConverter.toShort(value);
    }

    public static List<String> toStringArray(Object value) {
        return typeConverter.toStringArray(value);
    }

    public static String toString(Object value) {
        return typeConverter.toString(value);
    }

    public static Byte toByte(Object value) {
        return typeConverter.toByte(value);
    }

    public static Double toDouble(Object value) {
        return typeConverter.toDouble(value);
    }

    public static Float toFloat(Object value) {
        return typeConverter.toFloat(value);
    }

    public static Boolean toBoolean(Object value) {
        return typeConverter.toBoolean(value);
    }

    public static String toJson(Object obj) {
        return TapSimplify.toJson(obj);
    }

    public static DataMap fromJson(String json) {
        return TapSimplify.fromJson(json);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return TapSimplify.fromJson(json, clazz);
    }

    public static String format(String message, Object... args) {
        return FormatUtils.format(message, args);
    }

    public static TapField field(String name, String originType) {
        return TapSimplify.field(name, originType);
    }

    public static TapTable table(String tableName, String id) {
        return TapSimplify.table(tableName, id);
    }

    public static TapTable table(String nameAndId) {
        return TapSimplify.table(nameAndId);
    }

    public static TapString tapString() {
        return TapSimplify.tapString();
    }

    public static TapNumber tapNumber() {
        return TapSimplify.tapNumber();
    }

    public static TapRaw tapRaw() {
        return TapSimplify.tapRaw();
    }

    public static TapArray tapArray() {
        return TapSimplify.tapArray();
    }

    public static TapMap tapMap() {
        return TapSimplify.tapMap();
    }

    public static TapYear tapYear() {
        return TapSimplify.tapYear();
    }

    public static TapDate tapDate() {
        return TapSimplify.tapDate();
    }

    public static TapBoolean tapBoolean() {
        return TapSimplify.tapBoolean();
    }

    public static TapBinary tapBinary() {
        return TapSimplify.tapBinary();
    }

    public static TapTime tapTime() {
        return TapSimplify.tapTime();
    }

    public static TapDateTime tapDateTime() {
        return TapSimplify.tapDateTime();
    }

    public static TestItem testItem(String item, int resultCode) {
        return testItem(item, resultCode, null);
    }
    public static TestItem testItem(String item, int resultCode, String information) {
        return new TestItem(item, resultCode, information);
    }

    public static Entry entry(String key, Object value) {
        return TapSimplify.entry(key, value);
    }

    public static <T> List<T> list(T... ts) {
        return TapSimplify.list(ts);
    }

    public static <T> List<T> list() {
        return TapSimplify.list();
    }

    public static Map<String, Object> map() {
        return TapSimplify.map();
    }

    public static Map<String, Object> map(Entry... entries) {
        return TapSimplify.map(entries);
    }

    public static TapInsertRecordEvent insertRecordEvent(Map<String, Object> after, TapTable tapTable) {
        return TapSimplify.insertRecordEvent(after, tapTable);
    }

    public static TapDeleteRecordEvent deleteDMLEvent(Map<String, Object> before, TapTable tapTable) {
        return TapSimplify.deleteDMLEvent(before, tapTable);
    }

    public static TapUpdateRecordEvent updateDMLEvent(Map<String, Object> before, Map<String, Object> after, TapTable tapTable) {
        return TapSimplify.updateDMLEvent(before, after, tapTable);
    }

    public static WriteListResult<TapRecordEvent> writeListResult() {
        return new WriteListResult<>();
    }

    public static void sleep(long milliseconds) {
        TapSimplify.sleep(milliseconds);
    }

    public static String formatTapDateTime(DateTime dateTime, String pattern) {
        if (dateTime.getTimeZone() != null) dateTime.setTimeZone(dateTime.getTimeZone());
        tapDateTimeFormat.applyPattern(pattern);
        return tapDateTimeFormat.format(new Date(dateTime.getSeconds() * 1000L));
    }

    public static Object convertDateTimeToDate(DateTime dateTime) {
        return TapSimplify.convertDateTimeToDate(dateTime);
    }
}
