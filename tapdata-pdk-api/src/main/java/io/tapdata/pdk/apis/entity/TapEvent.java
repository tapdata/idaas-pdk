package io.tapdata.pdk.apis.entity;

import java.util.LinkedHashMap;
import java.util.Map;

public class TapEvent {
    /**
     * The time when the event is created
     */
    private Long time;
    /**
     * The reference time read from source, maybe some difference as sources are different
     * Used for CDC in most cases.
     * <p>
     * For example, MongoDB as source, when initial stage, referenceTime is null, when cdc stage, referenceTime is the clusterTime read from CDC stream
     */
    private Long referenceTime;

    private String pdkId;
    private String pdkGroup;

    public Map<String, Object> info;

    public Map<String, Object> traceMap;

    public String getPdkId() {
        return pdkId;
    }

    public void setPdkId(String pdkId) {
        this.pdkId = pdkId;
    }

    public String getPdkGroup() {
        return pdkGroup;
    }

    public void setPdkGroup(String pdkGroup) {
        this.pdkGroup = pdkGroup;
    }

    public Map<String, Object> getTraceMap() {
        return traceMap;
    }

    public void setTraceMap(Map<String, Object> traceMap) {
        this.traceMap = traceMap;
    }

    public Map<String, Object> getInfo() {
        return info;
    }

    public void setInfo(Map<String, Object> info) {
        this.info = info;
    }


    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getReferenceTime() {
        return referenceTime;
    }

    public void setReferenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
    }

}
