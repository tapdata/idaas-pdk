package io.tapdata.base;

import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.utils.FormatUtils;
import io.tapdata.pdk.apis.utils.ImplementationUtils;
import io.tapdata.pdk.apis.utils.TapUtils;
import io.tapdata.pdk.apis.utils.TypeConverter;
import org.toilelibre.libe.curl.Curl;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
}
