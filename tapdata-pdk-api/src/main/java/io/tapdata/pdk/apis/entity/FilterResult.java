package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class FilterResult {
    private TapFilter filter;

    private Map<String, Object> result;

    public TapFilter getFilter() {
        return filter;
    }

    public void setFilter(TapFilter filter) {
        this.filter = filter;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }
}
