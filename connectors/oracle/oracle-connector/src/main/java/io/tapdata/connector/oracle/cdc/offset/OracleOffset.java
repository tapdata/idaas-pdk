package io.tapdata.connector.oracle.cdc.offset;

import java.io.Serializable;

public class OracleOffset implements Serializable {

    private String sortString;
    private Long offsetValue;

    public OracleOffset() {

    }

    public OracleOffset(String sortString, Long offsetValue) {
        this.sortString = sortString;
        this.offsetValue = offsetValue;
    }

    public String getSortString() {
        return sortString;
    }

    public void setSortString(String sortString) {
        this.sortString = sortString;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }
}
