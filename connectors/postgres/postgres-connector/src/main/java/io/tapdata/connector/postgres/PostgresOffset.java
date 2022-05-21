package io.tapdata.connector.postgres;

import java.util.List;

/**
 * offset for batch read
 *
 * @author Jarad
 * @date 2022/5/09
 */
public class PostgresOffset {

    private String sortString;
    private Long offsetValue;
    private List<String> addTables;

    public PostgresOffset() {
    }

    public PostgresOffset(String sortString, Long offsetValue) {
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
