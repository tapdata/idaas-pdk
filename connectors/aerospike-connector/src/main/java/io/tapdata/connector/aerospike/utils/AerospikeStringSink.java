package io.tapdata.connector.aerospike.utils;

import io.tapdata.connector.aerospike.bean.IRecord;
import io.tapdata.connector.aerospike.bean.KeyValue;

public class AerospikeStringSink extends AerospikeAbstractSink<String, String> {
    public AerospikeStringSink() {
    }

    public KeyValue<String, String> extractKeyValue(IRecord<String> IRecord) {
        String key = IRecord.getKey().orElseGet(IRecord::getValue);
        return new KeyValue(key, IRecord.getValue());
    }
}
