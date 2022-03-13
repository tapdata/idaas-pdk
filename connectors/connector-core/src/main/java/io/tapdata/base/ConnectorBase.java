package io.tapdata.base;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.type.TapNumber;
import io.tapdata.entity.type.TapString;
import io.tapdata.entity.type.TapType;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.utils.FormatUtils;
import io.tapdata.pdk.apis.utils.ImplementationUtils;
import io.tapdata.pdk.apis.utils.TapUtils;
import io.tapdata.pdk.apis.utils.TypeConverter;
import org.toilelibre.libe.curl.Curl;

import java.util.*;

public class ConnectorBase {
    private TapUtils tapUtils = ImplementationUtils.getTapUtils();
    private TypeConverter typeConverter = ImplementationUtils.getTypeConverter();

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
        return tapUtils.toJson(obj);
    }

    public DefaultMap fromJson(String json) {
        return tapUtils.fromJson(json);
    }

    public <T> T fromJson(String json, Class<T> clazz) {
        return tapUtils.fromJson(json, clazz);
    }

    public String format(String message, Object... args) {
        return FormatUtils.format(message, args);
    }

    public TapField field(String name, TapType tapType) {
        return new TapField(name, tapType);
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

    public <T> List<T> list(T... ts) {
        return Arrays.asList(ts);
    }

    public enum TestResult {
        Successfully(TestItem.RESULT_SUCCESSFULLY),
        SuccessfullyWithWarn(TestItem.RESULT_SUCCESSFULLY_WITH_WARN),
        Failed(TestItem.RESULT_FAILED)
        ;
        int code;
        TestResult(int code) {
            this.code = code;
        }
    }

    public static final String ITEM_RESULT_PASS = "pass";
    public static final String ITEM_RESULT_FAILED = "failed";
    public TestItem testItem(String item, TestResult testResult) {
        return testItem(item, testResult, null);
    }
    public TestItem testItem(String item, TestResult testResult, String information) {
        return new TestItem(item, testResult.code, information);
    }
}
