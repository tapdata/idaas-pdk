package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.Consumer;

public interface ControlFunction {

    /**
     * insert, update, delete events.
     *
     * @param connectorContext
     * @param controlEvent
     *
     */
    void control(TapConnectorContext connectorContext, ControlEvent controlEvent) throws Throwable;

}
