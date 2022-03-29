package io.tapdata.entity.event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TapEvent implements Serializable {
    /**
     * The time when the event is created
     */
    protected Long time;

    public Map<String, Object> info;

    public Map<String, Object> traceMap;

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

    public void clone(TapEvent tapEvent) {
        tapEvent.time = time;
        if(info != null)
            tapEvent.info = new ConcurrentHashMap<>(info);
        if(traceMap != null)
            tapEvent.traceMap = new ConcurrentHashMap<>(traceMap);
    }

}
