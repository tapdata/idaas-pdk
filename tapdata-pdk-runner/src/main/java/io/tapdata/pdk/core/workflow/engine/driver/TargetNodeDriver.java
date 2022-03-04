package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.pdk.apis.functions.target.DMLFunction;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.pdk.core.utils.queue.ListHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TargetNodeDriver implements ListHandler<List<TapEvent>> {
    private static final String TAG = TargetNodeDriver.class.getSimpleName();

    private TargetNode targetNode;

    private AtomicBoolean connected = new AtomicBoolean(false);

    @Override
    public void execute(List<List<TapEvent>> list) throws Throwable {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
//        if(connected.compareAndSet(false, true)) {
//            ConnectFunction connectFunction = targetNode.getTargetFunctions().getConnectFunction();
//            if(connectFunction != null) {
//                pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_CONNECT, () -> {
//                    connectFunction.connect(targetNode.getConnectorContext());
//                }, "connect " + LoggerUtils.targetNodeMessage(targetNode), logger);
//            }
//        }

        for(List<TapEvent> events : list) {
            List<TapRecordEvent> recordEvents = new ArrayList<>();
            for (TapEvent event : events) {
                if(event instanceof TapRecordEvent) {
                    recordEvents.add((TapRecordEvent) event);
                }
            }
            DMLFunction insertRecordFunction = targetNode.getTargetFunctions().getDmlFunction();
            if(insertRecordFunction != null) {
                PDKLogger.debug(TAG, "Insert {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_INSERT, () -> {
                    insertRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents, (event, throwable) -> {
                        if(throwable != null) {
                            PDKLogger.error(TAG, "Insert record failed, {}, record size, {}, {}", throwable.getMessage(), event.getInsertedCount(), LoggerUtils.targetNodeMessage(targetNode));
                        } else {
                            PDKLogger.debug(TAG, "Inserted {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
                        }
                    });
                }, "insert " + LoggerUtils.targetNodeMessage(targetNode), TAG);
            }
        }
    }

    public TargetNode getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(TargetNode targetNode) {
        this.targetNode = targetNode;
    }
}
