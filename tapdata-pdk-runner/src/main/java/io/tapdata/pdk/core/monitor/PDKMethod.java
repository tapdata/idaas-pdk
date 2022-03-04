package io.tapdata.pdk.core.monitor;

import java.util.concurrent.TimeUnit;

public enum PDKMethod {
    SOURCE_INIT(TimeUnit.SECONDS.toMillis(5)),
    TARGET_INIT(TimeUnit.SECONDS.toMillis(5)),
    SOURCE_TARGET_INIT(TimeUnit.SECONDS.toMillis(5)),
    PROCESSOR_INIT(TimeUnit.SECONDS.toMillis(3)),

    SOURCE_FUNCTIONS(TimeUnit.SECONDS.toMillis(3)),
    TARGET_FUNCTIONS(TimeUnit.SECONDS.toMillis(3)),
    PROCESSOR_FUNCTIONS(TimeUnit.SECONDS.toMillis(3)),

    SOURCE_CONNECT(TimeUnit.SECONDS.toMillis(30)),
    TARGET_CONNECT(TimeUnit.SECONDS.toMillis(30)),

    SOURCE_DISCONNECT(TimeUnit.SECONDS.toMillis(30)),
    TARGET_DISCONNECT(TimeUnit.SECONDS.toMillis(30)),

    SOURCE_TYPE_MAPPING(TimeUnit.SECONDS.toMillis(10)),
    TARGET_TYPE_MAPPING(TimeUnit.SECONDS.toMillis(10)),

    SOURCE_VALUE_CLASS(TimeUnit.SECONDS.toMillis(10)),
    TARGET_VALUE_CLASS(TimeUnit.SECONDS.toMillis(10)),

    SOURCE_CONNECTION_TEST(TimeUnit.SECONDS.toMillis(10)),
    TARGET_CONNECTION_TEST(TimeUnit.SECONDS.toMillis(10)),

    SOURCE_BATCH_COUNT(TimeUnit.SECONDS.toMillis(30)),

    SOURCE_BATCH_READ,

    TARGET_INSERT(TimeUnit.SECONDS.toMillis(10)),

    TARGET_DML(TimeUnit.SECONDS.toMillis(10)),

    PROCESSOR_PROCESS_RECORD(TimeUnit.SECONDS.toMillis(10)),
    SOURCE_STREAM_READ;

    PDKMethod() {

    }
    PDKMethod(Long warnMilliseconds) {
        this.warnMilliseconds = warnMilliseconds;
    }
    private Long warnMilliseconds;
}
