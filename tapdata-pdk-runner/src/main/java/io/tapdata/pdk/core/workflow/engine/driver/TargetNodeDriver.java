package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
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

    private AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void execute(List<List<TapEvent>> list) throws Throwable {
        if(started.compareAndSet(false, true)) {
            targetNode.getConnector().discoverSchema(targetNode.getConnectorContext(), (tables) -> {
                if(tables != null) {
                    for(TapTable table : tables) {
                        if(table != null) {
                            TapTable targetTable = targetNode.getConnectorContext().getTable();
                            if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                                targetNode.getConnectorContext().setTable(table);
                                break;
                            }
                        }
                    }
                }
            });
        }
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
                if(event instanceof TapInsertRecordEvent) {
                    recordEvents.add(filterInsertEvent((TapInsertRecordEvent) event));
                }
            }
            WriteRecordFunction insertRecordFunction = targetNode.getConnectorFunctions().getDmlFunction();
            if(insertRecordFunction != null) {
                PDKLogger.debug(TAG, "Insert {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_INSERT, () -> {
                    insertRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents, (event) -> {
                        PDKLogger.debug(TAG, "Inserted {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
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

    private List<TapEvent> filterEvents(List<TapEvent> events) {
        for(TapEvent tapEvent : events) {
            if(tapEvent instanceof TapInsertRecordEvent) {
                filterInsertEvent((TapInsertRecordEvent) tapEvent);
            } else if(tapEvent instanceof TapUpdateRecordEvent) {
                filterUpdateEvent((TapUpdateRecordEvent) tapEvent);
            } else if(tapEvent instanceof TapDeleteRecordEvent) {
                filterDeleteEvent((TapDeleteRecordEvent) tapEvent);
            }
        }
        return events;
    }

    private TapDeleteRecordEvent filterDeleteEvent(TapDeleteRecordEvent deleteDMLEvent) {
        TapCodecFilterManager codecFilterManager = targetNode.getCodecFilterManager();
        codecFilterManager.transformFromTapValueMap(deleteDMLEvent.getBefore());
        return deleteDMLEvent;
    }

    private TapUpdateRecordEvent filterUpdateEvent(TapUpdateRecordEvent updateDMLEvent) {
        TapCodecFilterManager codecFilterManager = targetNode.getCodecFilterManager();
        codecFilterManager.transformFromTapValueMap(updateDMLEvent.getAfter());
        codecFilterManager.transformFromTapValueMap(updateDMLEvent.getBefore());
        return updateDMLEvent;
    }

    private TapInsertRecordEvent filterInsertEvent(TapInsertRecordEvent insertDMLEvent) {
        TapCodecFilterManager codecFilterManager = targetNode.getCodecFilterManager();
        codecFilterManager.transformFromTapValueMap(insertDMLEvent.getAfter());
        return insertDMLEvent;
    }
}
