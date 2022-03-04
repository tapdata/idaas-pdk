package io.tapdata.pdk.apis.functions.consumers;

import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;

/**
 * Record event actually write successfully
 *
 * @param <E>
 */
public interface TapWriteListConsumer<E> {
    /**
     * If throwable is null, means the event has been successfully write, otherwise the event insert failed
     * @param writeListResult
     * @param throwable
     */
    void accept(WriteListResult<E> writeListResult, Throwable throwable);
}
