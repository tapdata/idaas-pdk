package io.tapdata.pdk.apis.utils;

import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
//XPath
public class TapJsonMap extends LinkedHashMap<String, Object> {
    public String getString(String path) {
        return "";
    }

    public Integer getInteger(String path) {
        return 0;
    }

    //不太好实现
    public List<TapRecordEvent> getTapRecordEvents(String path) {
        Map<String, Object> json = null;
        TapRecordEvent recordEvent = new TapRecordEvent();
        recordEvent.setAfter(json);

        return null;
    }
    public TapRecordEvent getTapRecordEvent(String path) {
        return null;
    }

}
