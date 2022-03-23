package io.tapdata.base;

import io.tapdata.base.utils.Entry;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.type.*;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.utils.FormatUtils;
import io.tapdata.pdk.apis.utils.TapUtils;
import io.tapdata.pdk.apis.utils.TypeConverter;
import org.toilelibre.libe.curl.Curl;

import java.util.*;

public class ConnectorBase {
    private TapUtils tapUtils = InstanceFactory.instance(TapUtils.class);
    private TypeConverter typeConverter = InstanceFactory.instance(TypeConverter.class);
    private JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

    /**
     * @XXX
     * This CURL lib has bug, need bug fixing
     *
     * @param curl
     * @param params
     * @return
     */
    public DefaultMap curl(String curl, Object... params) {
        String result = Curl.$(format(curl, params));
        return fromJson(result, DefaultMap.class);
    }

    public void interval(Runnable runnable, int seconds) {
        tapUtils.interval(runnable, seconds);
    }

    public Long toLong(Object value) {
        return typeConverter.toLong(value);
    }

    public Integer toInteger(Object value) {
        return typeConverter.toInteger(value);
    }

    public Short toShort(Object value) {
        return typeConverter.toShort(value);
    }

    public List<String> toStringArray(Object value) {
        return typeConverter.toStringArray(value);
    }

    public String toString(Object value) {
        return typeConverter.toString(value);
    }

    public Byte toByte(Object value) {
        return typeConverter.toByte(value);
    }

    public Double toDouble(Object value) {
        return typeConverter.toDouble(value);
    }

    public Float toFloat(Object value) {
        return typeConverter.toFloat(value);
    }

    public Boolean toBoolean(Object value) {
        return typeConverter.toBoolean(value);
    }

    public String toJson(Object obj) {
        return jsonParser.toJson(obj);
    }

    public DefaultMap fromJson(String json) {
        return jsonParser.fromJson(json);
    }

    public <T> T fromJson(String json, Class<T> clazz) {
        return jsonParser.fromJson(json, clazz);
    }

    public String format(String message, Object... args) {
        return FormatUtils.format(message, args);
    }

    public TapField field(String name, String originType) {
        return new TapField(name, originType);
    }

    public TapTable table(String tableName, String id) {
        return new TapTable(tableName, id);
    }

    public TapTable table(String nameAndId) {
        return new TapTable(nameAndId);
    }

    public TapString tapString() {
        return new TapString();
    }

    public TapNumber tapNumber() {
        return new TapNumber();
    }

    public TapRaw tapRaw() {
        return new TapRaw();
    }

    public TapArray tapArray() {
        return new TapArray();
    }

    public TapMap tapMap() {
        return new TapMap();
    }

    public TapDate tapDate() {
        return new TapDate();
    }

    public TapBoolean tapBoolean() {
        return new TapBoolean();
    }

    public TapBinary tapBinary() {
        return new TapBinary();
    }

    public TapTime tapTime() {
        return new TapTime();
    }

    public TapDateTime tapDateTime() {
        return new TapDateTime();
    }

    public TestItem testItem(String item, int resultCode) {
        return testItem(item, resultCode, null);
    }
    public TestItem testItem(String item, int resultCode, String information) {
        return new TestItem(item, resultCode, information);
    }

    public Entry entry(String key, Object value) {
        return new Entry(key, value);
    }

    public <T> List<T> list(T... ts) {
        return Arrays.asList(ts);
    }

    public <T> List<T> list() {
        return new ArrayList<T>();
    }

    public Map<String, Object> map() {
        return new LinkedHashMap<>();
    }

    public Map<String, Object> map(Entry... entries) {
        Map<String, Object> map = new LinkedHashMap<>();
        if(entries != null) {
            for(Entry entry : entries) {
                if(entry.getKey() != null && entry.getValue() != null)
                    map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public TapInsertRecordEvent insertRecordEvent(Map<String, Object> after, TapTable tapTable) {
        return new TapInsertRecordEvent().init().after(after).table(tapTable);
    }

    public TapDeleteRecordEvent deleteDMLEvent(Map<String, Object> before, TapTable tapTable) {
        return new TapDeleteRecordEvent().init().before(before).table(tapTable);
    }

    public TapUpdateRecordEvent updateDMLEvent(Map<String, Object> before, Map<String, Object> after, TapTable tapTable) {
        return new TapUpdateRecordEvent().init().before(before).after(after).table(tapTable);
    }

    public WriteListResult<TapRecordEvent> writeListResult() {
        return new WriteListResult<TapRecordEvent>();
    }

    public void sleep(long milliseconds) {
        if(milliseconds < 0)
            return;
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
    }
}
