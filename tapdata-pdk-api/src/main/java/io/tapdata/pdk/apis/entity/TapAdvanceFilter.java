package io.tapdata.pdk.apis.entity;

/**
 * >, >=, <, <=
 * or, and
 */
public class TapAdvanceFilter extends TapFilter {
    private Integer limit;

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
