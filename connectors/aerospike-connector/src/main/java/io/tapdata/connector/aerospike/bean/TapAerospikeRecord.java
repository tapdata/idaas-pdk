package io.tapdata.connector.aerospike.bean;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.apis.common.DefaultMap;

import java.util.Map;
import java.util.Optional;

public class TapAerospikeRecord implements IRecord<String> {
    private DefaultMap binValuesMap; // 对于行数据，拆分为键值对
    private String record; // 记录读入的所有信息
    private String key;

    private TapAerospikeRecord() {
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
