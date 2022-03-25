package io.tapdata.pdk.core.workflow.engine.driver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
                            analyzeTableFields(table);
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
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_READ,
                    () -> batchReadFunction.batchRead(sourceNode.getConnectorContext(), finalRecoveredOffset, 100, (events) -> {
                        if (events != null && !events.isEmpty()) {
                            PDKLogger.debug(TAG, "Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offer(filterEvents(events));

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
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
                while(true) {
                    streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events) -> {
                        if (events != null) {
                            PDKLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offer(filterEvents(events));
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

    private void analyzeTableFields(TapTable table) {
        sourceNode.getConnectorContext().setTable(table);

        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if(nameFieldMap != null) {
            DefaultExpressionMatchingMap expressionMatchingMap = sourceNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap();
            for(Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                if(entry.getValue().getOriginType() != null) {
                    TypeExprResult<DefaultMap> result = expressionMatchingMap.get(entry.getValue().getOriginType());
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

    private List<TapEvent> filterEvents(List<TapEvent> events) {
        LinkedHashMap<String, TapField> nameFieldMap = sourceNode.getConnectorContext().getTable().getNameFieldMap();
        TapCodecFilterManager codecFilterManager = sourceNode.getCodecFilterManager();
        for(TapEvent tapEvent : events) {
            if(tapEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertDMLEvent = (TapInsertRecordEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(insertDMLEvent.getAfter(), nameFieldMap);
            } else if(tapEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent updateDMLEvent = (TapUpdateRecordEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(updateDMLEvent.getAfter(), nameFieldMap);
                codecFilterManager.transformToTapValueMap(updateDMLEvent.getBefore(), nameFieldMap);
            } else if(tapEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteDMLEvent = (TapDeleteRecordEvent) tapEvent;
                codecFilterManager.transformToTapValueMap(deleteDMLEvent.getBefore(), nameFieldMap);
            }
        }
        return events;
    }
}
