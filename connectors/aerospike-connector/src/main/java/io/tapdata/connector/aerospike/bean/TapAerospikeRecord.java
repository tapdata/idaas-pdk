package io.tapdata.connector.aerospike.bean;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.apis.common.DefaultMap;

import java.util.Map;
import java.util.Optional;

public class TapAerospikeRecord implements IRecord<String> {
    private DefaultMap binValuesMap;
    private String record;
    private String key;

    private TapAerospikeRecord() {
    }

    public TapAerospikeRecord(String json) {
        this.binValuesMap = JSON.parseObject(json, DefaultMap.class);
        this.record = json;
    }

    public TapAerospikeRecord(String json, String key) {
        this.binValuesMap = JSON.parseObject(json, DefaultMap.class);
        this.record = json;
        this.key = key;
    }

    @Override
    public String getValue() {
        return this.record;
    }

    @Override
    public Map getBinValuesMap(){
        return this.binValuesMap;
    }

    public Optional<String> getKey() {
        if (this.key != null) {
            return Optional.of(this.key);
        }
        return Optional.empty();
    }

}
