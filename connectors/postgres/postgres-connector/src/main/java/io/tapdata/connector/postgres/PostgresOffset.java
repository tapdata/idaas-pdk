package io.tapdata.connector.postgres;

/**
 * offset for batch read
 *
 * @author Jarad
 * @date 2022/5/09
 */
public class PostgresOffset {

    private String sortString;
    private Long offsetValue;

    private String streamOffsetKey;
    private String streamOffsetValue;
    private Long streamOffsetTime;

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

    public String getStreamOffsetKey() {
        return streamOffsetKey;
    }

    public void setStreamOffsetKey(String streamOffsetKey) {
        this.streamOffsetKey = streamOffsetKey;
    }

    public String getStreamOffsetValue() {
        return streamOffsetValue;
    }

    public void setStreamOffsetValue(String streamOffsetValue) {
        this.streamOffsetValue = streamOffsetValue;
    }

    public Long getStreamOffsetTime() {
        return streamOffsetTime;
    }

    public void setStreamOffsetTime(Long streamOffsetTime) {
        this.streamOffsetTime = streamOffsetTime;
    }
}
