package io.tapdata.pdk.apis.consumer;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.utils.StateListener;

import java.util.List;
import java.util.function.Consumer;

public class StreamReadConsumer implements Consumer<List<TapEvent>> {
    public static final int STATE_STREAM_READ_PENDING = 1;
    public static final int STATE_STREAM_READ_STARTED = 10;
    public static final int STATE_STREAM_READ_STARTED_ASYNC = 20;
    public static final int STATE_STREAM_READ_ENDED = 100;
    private int state = STATE_STREAM_READ_PENDING;

    private Consumer<List<TapEvent>> consumer;
    private StateListener<Integer> stateListener;
    private boolean asyncMethodAndNoRetry;

    public synchronized void streamReadStarted() {
        streamReadStarted(false);
    }

    public synchronized void streamReadStarted(boolean asyncMethodAndNoRetry) {
        if(state == STATE_STREAM_READ_STARTED)
            return;
        this.asyncMethodAndNoRetry = asyncMethodAndNoRetry;

        int old = state;
        if(this.asyncMethodAndNoRetry) {
            state = STATE_STREAM_READ_STARTED_ASYNC;
        } else {
            state = STATE_STREAM_READ_STARTED;
        }
        if(stateListener != null) {
            stateListener.stateChanged(old, state);
        }
    }

    public synchronized void streamReadEnded() {
        if(state == STATE_STREAM_READ_ENDED)
            return;

        int old = state;
        state = STATE_STREAM_READ_ENDED;
        if(stateListener != null) {
            stateListener.stateChanged(old, state);
        }
    }

    public int getState() {
        return state;
    }

    public static StreamReadConsumer create(Consumer<List<TapEvent>> consumer) {
        return new StreamReadConsumer().consumer(consumer);
    }

    private StreamReadConsumer consumer(Consumer<List<TapEvent>> consumer) {
        this.consumer = consumer;
        return this;
    }

    public StreamReadConsumer stateListener(StateListener<Integer> stateListener) {
        this.stateListener = stateListener;
        return this;
    }

    @Override
    public void accept(List<TapEvent> events) {
        consumer.accept(events);
    }
}
