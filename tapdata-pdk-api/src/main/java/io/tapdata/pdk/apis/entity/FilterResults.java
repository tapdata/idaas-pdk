package io.tapdata.pdk.apis.entity;

import java.util.List;
import java.util.Map;

public class FilterResults {
    private TapFilter filter;

    private List<Map<String, Object>> results;

    public TapFilter getFilter() {
        return filter;
    }

    public void setFilter(TapFilter filter) {
        this.filter = filter;
    }

    public List<Map<String, Object>> getResults() {
        return results;
    }

    public void setResults(List<Map<String, Object>> results) {
        this.results = results;
    }
}
