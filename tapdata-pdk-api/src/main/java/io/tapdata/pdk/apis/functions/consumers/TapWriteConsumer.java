package io.tapdata.pdk.apis.functions.consumers;

/**
 * Record event actually write successfully
 *
 * @param <E>
 */
public interface TapWriteConsumer<E> {
    /**
     * If throwable is null, means the event has been successfully write, otherwise the event insert failed
     *
     * @param event
     * @param throwable
     */
    void accept(E event, Throwable throwable);
}
