package io.tapdata.pdk.core.workflow.engine.driver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteDMLEvent;
import io.tapdata.entity.event.dml.TapInsertDMLEvent;
import io.tapdata.entity.event.dml.TapUpdateDMLEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;

import java.util.LinkedHashMap;
import java.util.List;

public class SourceNodeDriver extends Driver {
    private static final String TAG = SourceNodeDriver.class.getSimpleName();

    private SourceNode sourceNode;

    private String streamOffsetStr;
    private String batchOffsetStr;
    private int batchLimit = 100;
    private Long batchCount;
    private boolean batchCompleted = false;
    private final Object streamLock = new int[0];

    public SourceNode getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(SourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public void start() {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();

        //Fill the discovered table back into connector context
        //The table user input has to be one in the discovered tables. Otherwise we need create table logic which currently we don't have.
        sourceNode.getConnector().discoverSchema(sourceNode.getConnectorContext(), (tables) -> {
            if(tables != null) {
                for(TapTable table : tables) {
                    if(table != null) {
                        TapTable targetTable = sourceNode.getConnectorContext().getTable();
                        if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                            sourceNode.getConnectorContext().setTable(table);
                            break;
                        }
                    }
                }
            }
        });

        BatchCountFunction batchCountFunction = sourceNode.getConnectorFunctions().getBatchCountFunction();
        if (batchCountFunction != null) {
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_COUNT, () -> {
                batchCount = batchCountFunction.count(sourceNode.getConnectorContext(), null);
            }, "Batch count " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
        }

        StreamReadFunction streamReadFunction = sourceNode.getConnectorFunctions().getStreamReadFunction();
        if (streamReadFunction != null) {
            Object recoveredOffset = null;
            if(streamOffsetStr != null) {
                recoveredOffset = JSON.parse(streamOffsetStr, Feature.SupportAutoType);
            }

            //TODO 提供方法返回增量断点， 不要使用wait的方式
            Object finalRecoveredOffset = recoveredOffset;
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
                while(true) {
                    streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events) -> {
                        if (!batchCompleted) {
                            synchronized (streamLock) {
                                while (!batchCompleted) {
                                    PDKLogger.debug(TAG, "Stream read will wait until batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                                    try {
                                        streamLock.wait();
                                    } catch (InterruptedException interruptedException) {
//                                    interruptedException.printStackTrace();
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                PDKLogger.debug(TAG, "Stream read start now, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                            }
                        }
                        if (events != null) {
                            PDKLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offer(filterEvents(events));
                        }
//                        if (offsetState != null) {
//                            PDKLogger.debug(TAG, "Stream read update offset from {} to {}", this.streamOffsetStr, offsetState);
//                            this.streamOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
//                        }
                    });
                }
            }, "connect " + LoggerUtils.sourceNodeMessage(sourceNode), TAG, true, Long.MAX_VALUE, 5);
        }

        BatchReadFunction batchReadFunction = sourceNode.getConnectorFunctions().getBatchReadFunction();
        if (batchReadFunction != null) {
            Object recoveredOffset = null;
            if(batchOffsetStr != null) {
                recoveredOffset = JSON.parse(batchOffsetStr, Feature.SupportAutoType);
            }
            Object finalRecoveredOffset = recoveredOffset;
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_READ,
                    () -> batchReadFunction.batchRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events) -> {
                        if (events != null && !events.isEmpty()) {
                            PDKLogger.debug(TAG, "Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offer(filterEvents(events));

//                                if (offsetState != null) {
//                                    PDKLogger.debug(TAG, "Batch read update offset from {} to {}", this.batchOffsetStr, offsetState);
//                                    batchOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
//                                }
                        }
                    }), "Batch read " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            if (!batchCompleted) {
                synchronized (streamLock) {
                    if (!batchCompleted) {
                        batchCompleted = true;
                        PDKLogger.debug(TAG, "Batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                        streamLock.notifyAll();
                    }
                }
            }
        }
    }

    private List<TapEvent> filterEvents(List<TapEvent> events) {
        LinkedHashMap<String, TapField> nameFieldMap = sourceNode.getConnectorContext().getTable().getNameFieldMap();
        TapCodecFilterManager codecFilterManager = sourceNode.getCodecFilterManager();
        for(TapEvent tapEvent : events) {
            if(tapEvent instanceof TapInsertDMLEvent) {
                TapInsertDMLEvent insertDMLEvent = (TapInsertDMLEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(insertDMLEvent.getAfter(), nameFieldMap);
            } else if(tapEvent instanceof TapUpdateDMLEvent) {
                TapUpdateDMLEvent updateDMLEvent = (TapUpdateDMLEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(updateDMLEvent.getAfter(), nameFieldMap);
                codecFilterManager.transformToTapValueMap(updateDMLEvent.getBefore(), nameFieldMap);
            } else if(tapEvent instanceof TapDeleteDMLEvent) {
                TapDeleteDMLEvent deleteDMLEvent = (TapDeleteDMLEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(deleteDMLEvent.getBefore(), nameFieldMap);
            }
        }
        return events;
    }
}
