package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.pretty.ClassHandlers;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class ConnectorNode extends Node {
    private static final String TAG = ConnectorNode.class.getSimpleName();
    TapConnector connector;
    TapCodecRegistry codecRegistry;
    TapConnectorContext connectorContext;

    ConnectorFunctions connectorFunctions;
    TapCodecFilterManager codecFilterManager;

    Queue<TapEvent> externalEvents;

    public void init(TapConnector tapNode, TapCodecRegistry codecRegistry, ConnectorFunctions connectorFunctions) {
        connector = tapNode;
        this.codecRegistry = codecRegistry;
        this.connectorFunctions = connectorFunctions;
        codecFilterManager = new TapCodecFilterManager(this.codecRegistry);
        externalEvents = new ConcurrentLinkedQueue<>();
    }

    public void init(TapConnector tapNode) {
        init(tapNode, new TapCodecRegistry(), new ConnectorFunctions());
    }

    public void offerExternalEvent(TapEvent tapEvent) {
        if(externalEvents != null) {
            if(tapEvent instanceof PatrolEvent) {
                PatrolEvent patrolEvent = (PatrolEvent) tapEvent;
                if(patrolEvent.applyState(getAssociateId(), PatrolEvent.STATE_ENTER)) {
                    if(patrolEvent.getPatrolListener() != null) {
                        CommonUtils.ignoreAnyError(() -> patrolEvent.getPatrolListener().patrol(getAssociateId(), PatrolEvent.STATE_ENTER), TAG);
                    }
                }
            }
            externalEvents.offer(tapEvent);
        }
    }

    public List<TapEvent> pullAllExternalEventsInList(Consumer<TapEvent> consumer) {
        if(externalEvents != null) {
            if(externalEvents.isEmpty()) return null;

            List<TapEvent> events = new ArrayList<>();
            TapEvent tapEvent;
            while((tapEvent = externalEvents.poll()) != null) {
                if(consumer != null)
                    consumer.accept(tapEvent);
                events.add(tapEvent);
            }
            return events;
        }
        return null;
    }

    public void pullAllExternalEvents(Consumer<TapEvent> consumer) {
        if(externalEvents != null) {
            TapEvent tapEvent;
            while((tapEvent = externalEvents.poll()) != null) {
                consumer.accept(tapEvent);
            }
        }
    }

    public TapCodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    public void registerCapabilities() {
        connector.registerCapabilities(connectorFunctions, codecRegistry);
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapConnector getConnector() {
        return connector;
    }

    public ConnectorFunctions getConnectorFunctions() {
        return connectorFunctions;
    }

    public TapCodecFilterManager getCodecFilterManager() {
        return codecFilterManager;
    }
}
