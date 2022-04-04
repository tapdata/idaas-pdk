package io.tapdata.connector.aerospike.bean;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public interface IRecord<T> {
    default Optional<String> getTopicName() {
        return Optional.empty();
    }

    default Optional<String> getKey() {
        return Optional.empty();
    }

    T getValue();

    default Optional<Long> getEventTime() {
        return Optional.empty();
    }

    default Optional<String> getPartitionId() {
        return Optional.empty();
    }

    default Optional<Long> getRecordSequence() {
        return Optional.empty();
    }

    default Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    default Map<String, Object> getBinValuesMap() {
        return Collections.emptyMap();
    }

    default void ack() {
    }

    default void fail() {
    }

    default Optional<String> getDestinationTopic() {
        return Optional.empty();
    }

//    default Optional<Message<T>> getMessage() {
//        return Optional.empty();
//    }
}
