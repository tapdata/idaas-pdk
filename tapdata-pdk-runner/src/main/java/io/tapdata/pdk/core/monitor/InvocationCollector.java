package io.tapdata.pdk.core.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class InvocationCollector {
    private PDKMethod pdkMethod;
    private Map<String, Long> invokeIdTimeMap = new ConcurrentHashMap<>();
    private LongAdder counter = new LongAdder();
    private LongAdder totalTakes = new LongAdder();

    public InvocationCollector(PDKMethod method) {
        pdkMethod = method;
    }

    public PDKMethod getPdkMethod() {
        return pdkMethod;
    }

    public void setPdkMethod(PDKMethod pdkMethod) {
        this.pdkMethod = pdkMethod;
    }

    public Map<String, Long> getInvokeIdTimeMap() {
        return invokeIdTimeMap;
    }

    public void setInvokeIdTimeMap(Map<String, Long> invokeIdTimeMap) {
        this.invokeIdTimeMap = invokeIdTimeMap;
    }

    public LongAdder getCounter() {
        return counter;
    }

    public void setCounter(LongAdder counter) {
        this.counter = counter;
    }

    public LongAdder getTotalTakes() {
        return totalTakes;
    }

    public void setTotalTakes(LongAdder totalTakes) {
        this.totalTakes = totalTakes;
    }
}
