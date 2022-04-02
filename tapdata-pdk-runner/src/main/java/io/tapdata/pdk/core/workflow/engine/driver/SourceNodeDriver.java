package io.tapdata.pdk.core.workflow.engine.driver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.ControlFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SourceNodeDriver extends Driver {
    private static final String TAG = SourceNodeDriver.class.getSimpleName();

    private SourceNode sourceNode;

    private String streamOffsetStr;
    private String batchOffsetStr;
    private SourceStateListener sourceStateListener;
    private int batchLimit = 1000;
    private Long batchCount;
    private boolean batchCompleted = false;
    private final Object streamLock = new int[0];
    private final AtomicBoolean firstBatchRecordsWillOffer = new AtomicBoolean(false);
    private final AtomicBoolean firstBatchRecordsOffered = new AtomicBoolean(false);

    public SourceNode getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(SourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public SourceStateListener getSourceStateListener() {
        return sourceStateListener;
    }

    public void setSourceStateListener(SourceStateListener sourceStateListener) {
        this.sourceStateListener = sourceStateListener;
    }
    public static final int STATE_STARTED = 1;
    public static final int STATE_FIRST_BATCH_RECORDS_OFFERED = 2;
    public static final int STATE_BATCH_STARTED = 10;
    public static final int STATE_BATCH_ENDED = 20;
    public static final int STATE_STREAM_STARTED = 30;
    public static final int STATE_ENDED = 100;
    public interface SourceStateListener {
        void stateChanged(int state);
    }
    public void start() {
        start(null);
    }
    public void start(SourceStateListener sourceStateListener) {
        this.sourceStateListener = sourceStateListener;
        CommonUtils.ignoreAnyError(() -> {
            if(sourceStateListener != null)
                sourceStateListener.stateChanged(STATE_STARTED);
        }, TAG);
        PDKLogger.info(TAG, "SourceNodeDriver started, {}", LoggerUtils.sourceNodeMessage(sourceNode));
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();

        //Fill the discovered table back into connector context
        //The table user input has to be one in the discovered tables. Otherwise we need create table logic which currently we don't have.
        pdkInvocationMonitor.invokePDKMethod(PDKMethod.DISCOVER_SCHEMA, () -> {
            sourceNode.getConnector().discoverSchema(sourceNode.getConnectorContext(), (tables) -> {
                if(tables == null) return;
                for(TapTable table : tables) {
                    if(table == null) continue;
                    TapTable targetTable = sourceNode.getConnectorContext().getTable();
                    if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                        analyzeTableFields(table);
                        break;
                    }
                }
            });
        }, "Discover schema " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);


        BatchCountFunction batchCountFunction = sourceNode.getConnectorFunctions().getBatchCountFunction();
        if (batchCountFunction != null) {
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_COUNT, () -> {
                batchCount = batchCountFunction.count(sourceNode.getConnectorContext(), null);
            }, "Batch count " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
        }

//        StreamReadFunction streamReadFunction = sourceNode.getConnectorFunctions().getStreamReadFunction();
//        if (streamReadFunction != null) {
//            Object recoveredOffset = null;
//            if(streamOffsetStr != null) {
//                recoveredOffset = JSON.parse(streamOffsetStr, Feature.SupportAutoType);
//            }
//
//            //TODO 提供方法返回增量断点， 不要使用wait的方式
//            Object finalRecoveredOffset = recoveredOffset;
//            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
//                while(true) {
//                    streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events) -> {
//                        if (!batchCompleted) {
//                            synchronized (streamLock) {
//                                while (!batchCompleted) {
//                                    PDKLogger.debug(TAG, "Stream read will wait until batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
//                                    try {
//                                        streamLock.wait();
//                                    } catch (InterruptedException interruptedException) {
////                                    interruptedException.printStackTrace();
//                                        Thread.currentThread().interrupt();
//                                    }
//                                }
//                                PDKLogger.debug(TAG, "Stream read start now, {}", LoggerUtils.sourceNodeMessage(sourceNode));
//                            }
//                        }
//                        if (events != null) {
//                            PDKLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
//                            offer(filterEvents(events));
//                        }
////                        if (offsetState != null) {
////                            PDKLogger.debug(TAG, "Stream read update offset from {} to {}", this.streamOffsetStr, offsetState);
////                            this.streamOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
////                        }
//                    });
//                }
//            }, "connect " + LoggerUtils.sourceNodeMessage(sourceNode), TAG, null, true, Long.MAX_VALUE, 5);
//        }

        BatchReadFunction batchReadFunction = sourceNode.getConnectorFunctions().getBatchReadFunction();
        if (batchReadFunction != null) {
            Object recoveredOffset = null;
            if(batchOffsetStr != null) {
                recoveredOffset = JSON.parse(batchOffsetStr, Feature.SupportAutoType);
            }
            Object finalRecoveredOffset = recoveredOffset;
            CommonUtils.ignoreAnyError(() -> {
                if(sourceStateListener != null)
                    sourceStateListener.stateChanged(STATE_BATCH_STARTED);
            }, TAG);
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_READ,
                    () -> batchReadFunction.batchRead(sourceNode.getConnectorContext(), finalRecoveredOffset, batchLimit, (events) -> {
                        if (events != null && !events.isEmpty()) {
                            PDKLogger.debug(TAG, "Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
//                            offer(events, (theEvents) -> filterEvents(theEvents));
                            offerToQueue(events);

//                            List<TapEvent> externalEvents = sourceNode.pullAllExternalEventsInList(this::filterExternalEvent);
//                            if(externalEvents != null) {
//                                PDKLogger.debug(TAG, "Batch read external {} of events, {}", externalEvents.size(), LoggerUtils.sourceNodeMessage(sourceNode));
//                                offer(externalEvents);
//                            }

                            BatchOffsetFunction batchOffsetFunction = sourceNode.getConnectorFunctions().getBatchOffsetFunction();
                            if(batchOffsetFunction != null) {
                                pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_OFFSET, () -> {
                                    Object offsetState = batchOffsetFunction.batchOffset(getSourceNode().getConnectorContext());
                                    if(offsetState != null) {
                                        PDKLogger.debug(TAG, "Batch read update offset from {} to {}", this.batchOffsetStr, offsetState);
                                        batchOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
                                    }
                                }, "Batch offset " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
                            }
                        }
                    }), "Batch read " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            if (!batchCompleted) {
                synchronized (streamLock) {
                    if (!batchCompleted) {
                        batchCompleted = true;
                        CommonUtils.ignoreAnyError(() -> {
                            if(sourceStateListener != null)
                                sourceStateListener.stateChanged(STATE_BATCH_ENDED);
                        }, TAG);
                        PDKLogger.debug(TAG, "Batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                    }
                }
            }
        }

        StreamReadFunction streamReadFunction = sourceNode.getConnectorFunctions().getStreamReadFunction();
        if (streamReadFunction != null) {
            Object recoveredOffset = null;
            if(streamOffsetStr != null) {
                recoveredOffset = JSON.parse(streamOffsetStr, Feature.SupportAutoType);
            }

            Object finalRecoveredOffset = recoveredOffset;
            CommonUtils.ignoreAnyError(() -> {
                if(sourceStateListener != null)
                    sourceStateListener.stateChanged(STATE_STREAM_STARTED);
            }, TAG);
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
                while(true) {
                    streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events) -> {
                        if (events != null) {
                            PDKLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offerToQueue(events);

//                            List<TapEvent> externalEvents = sourceNode.pullAllExternalEventsInList(this::filterExternalEvent);
//                            if(externalEvents != null) {
//                                PDKLogger.debug(TAG, "Stream read external {} of events, {}", externalEvents.size(), LoggerUtils.sourceNodeMessage(sourceNode));
//                                offer(externalEvents);
//                            }
                        }

                        StreamOffsetFunction streamOffsetFunction = sourceNode.getConnectorFunctions().getStreamOffsetFunction();
                        if(streamOffsetFunction != null) {
                            pdkInvocationMonitor.invokePDKMethod(PDKMethod.STREAM_OFFSET, () -> {
                                Object offsetState = streamOffsetFunction.streamOffset(sourceNode.getConnectorContext(), null);
                                if (offsetState != null) {
                                    PDKLogger.debug(TAG, "Stream read update offset from {} to {}", this.streamOffsetStr, offsetState);
                                    this.streamOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
                                }
                            }, "Stream read sourceNode " + sourceNode.getConnectorContext(), TAG, error -> {
                                PDKLogger.error("streamOffset failed, {} sourceNode {}", error.getMessage(), sourceNode.getConnectorContext());
                            });
                        }
                    });
                }
            }, "connect " + LoggerUtils.sourceNodeMessage(sourceNode), TAG, null, true, Long.MAX_VALUE, 5);
        }
    }

    private void offerToQueue(List<TapEvent> events) {
        offer(events, this::filterEvents);
        if(firstBatchRecordsWillOffer.get() && firstBatchRecordsOffered.compareAndSet(false, true)) {
            CommonUtils.ignoreAnyError(() -> {
                if(sourceStateListener != null)
                    sourceStateListener.stateChanged(STATE_FIRST_BATCH_RECORDS_OFFERED);
            }, TAG);
        }
    }

    public void receivedExternalEvent(List<TapEvent> events) {
        if(events == null)
            return;

        List<ControlEvent> controlEvents = new ArrayList<>();
//            targetNode.pullAllExternalEvents(tapEvent -> events.add(tapEvent));
        for (TapEvent event : events) {
            if(event instanceof ControlEvent) {
                controlEvents.add((ControlEvent) event);
            } else if(event instanceof TapBaseEvent) {
                ((TapBaseEvent) event).setTable(sourceNode.getConnectorContext().getTable());
            }
        }

        handleControlEvent(controlEvents);
//        offer(events, (theEvents) -> filterEvents(theEvents));
        offerToQueue(events);
    }

    private void handleControlEvent(List<ControlEvent> events) {
        if(events.isEmpty())
            return;
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        ControlFunction controlFunction = sourceNode.getConnectorFunctions().getControlFunction();

        PDKLogger.debug(TAG, "Handled {} of control events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
        for(ControlEvent controlEvent : events) {
            if(controlFunction != null) {
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.CONTROL, () -> {
                    controlFunction.control(sourceNode.getConnectorContext(), controlEvent);
                }, "control event " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            }

            if(controlEvent instanceof PatrolEvent) {
                PatrolEvent patrolEvent = (PatrolEvent) controlEvent;
                if(patrolEvent.applyState(sourceNode.getAssociateId(), PatrolEvent.STATE_LEAVE)) {
                    if(patrolEvent.getPatrolListener() != null) {
                        CommonUtils.ignoreAnyError(() -> patrolEvent.getPatrolListener().patrol(sourceNode.getAssociateId(), PatrolEvent.STATE_LEAVE), TAG);
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        CommonUtils.ignoreAnyError(() -> {
            if(sourceStateListener != null)
                sourceStateListener.stateChanged(STATE_ENDED);
        }, TAG);
    }

    public List<TapEvent> filterEvents(List<TapEvent> events) {
        LinkedHashMap<String, TapField> nameFieldMap = sourceNode.getConnectorContext().getTable().getNameFieldMap();
        TapCodecFilterManager codecFilterManager = sourceNode.getCodecFilterManager();
        List<TapEvent> newEvents = new ArrayList<>();
        for(TapEvent tapEvent : events) {
            if(tapEvent instanceof TapBaseEvent) {
                firstBatchRecordsWillOffer.compareAndSet(false, true);

                ((TapBaseEvent) tapEvent).setTable(sourceNode.getConnectorContext().getTable());
            }
            if(tapEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertDMLEvent = (TapInsertRecordEvent) tapEvent;
                TapInsertRecordEvent newInsertDMLEvent = new TapInsertRecordEvent();
                insertDMLEvent.clone(newInsertDMLEvent);
                codecFilterManager.transformToTapValueMap(newInsertDMLEvent.getAfter(), nameFieldMap);
                newEvents.add(newInsertDMLEvent);
            } else if(tapEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent updateDMLEvent = (TapUpdateRecordEvent) tapEvent;
                TapUpdateRecordEvent newUpdateDMLEvent = new TapUpdateRecordEvent();
                updateDMLEvent.clone(newUpdateDMLEvent);
                codecFilterManager.transformToTapValueMap(newUpdateDMLEvent.getAfter(), nameFieldMap);
                codecFilterManager.transformToTapValueMap(newUpdateDMLEvent.getBefore(), nameFieldMap);
                newEvents.add(newUpdateDMLEvent);
            } else if(tapEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteDMLEvent = (TapDeleteRecordEvent) tapEvent;
                TapDeleteRecordEvent newDeleteDMLEvent = new TapDeleteRecordEvent();
                deleteDMLEvent.clone(newDeleteDMLEvent);
                codecFilterManager.transformToTapValueMap(newDeleteDMLEvent.getBefore(), nameFieldMap);
                newEvents.add(newDeleteDMLEvent);
            } else {
                try {
                    TapEvent newTapEvent = tapEvent.getClass().getConstructor().newInstance();
                    tapEvent.clone(newTapEvent);
                    newEvents.add(newTapEvent);
                } catch (Throwable e) {
                    e.printStackTrace();
                    PDKLogger.error(TAG, "New instance for {} failed, {}. TapEvent {} will be ignored", tapEvent.getClass(), e.getMessage(), tapEvent);
                }
            }
        }
        return newEvents;
    }

    public void analyzeTableFields(TapTable table) {
        sourceNode.getConnectorContext().setTable(table);

        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if(nameFieldMap != null) {
            DefaultExpressionMatchingMap expressionMatchingMap = sourceNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap();
            for(Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                if(entry.getValue().getOriginType() != null) {
                    TypeExprResult<DataMap> result = expressionMatchingMap.get(entry.getValue().getOriginType());
                    if(result != null) {
                        TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
                        if(tapMapping != null) {
                            entry.getValue().setTapType(tapMapping.toTapType(entry.getValue().getOriginType(), result.getParams()));
                        }
                    } else {
                        PDKLogger.error(TAG, "Field originType {} didn't match corresponding TapMapping, please check your dataTypes json definition. {}", entry.getValue().getOriginType(), LoggerUtils.sourceNodeMessage(sourceNode));
                    }
                }
            }
        }
    }
}
