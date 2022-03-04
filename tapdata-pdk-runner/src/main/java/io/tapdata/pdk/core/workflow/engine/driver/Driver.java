package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class Driver {
    private List<SingleThreadBlockingQueue<List<TapEvent>>> queues = new CopyOnWriteArrayList<>();

    public void registerQueue(SingleThreadBlockingQueue<List<TapEvent>> queue) {
        if(!queues.contains(queue))
            queues.add(queue);
    }

    public void offer(List<TapEvent> tapEvents) {
        for(SingleThreadBlockingQueue<List<TapEvent>> queue : queues) {
            queue.offer(tapEvents);
        }
    }
}
