package io.tapdata.pdk.core.workflow.engine.driver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.pdk.apis.functions.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.source.StreamReadFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;

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
//        ConnectFunction connectFunction = sourceNode.getSourceFunctions().getConnectFunction();
//        if (connectFunction != null) {
//            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_CONNECT, () -> {
//                connectFunction.connect(sourceNode.getConnectorContext());
//            }, "connect", logger);
//        }

        BatchCountFunction batchCountFunction = sourceNode.getSourceFunctions().getBatchCountFunction();
        if (batchCountFunction != null) {
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_COUNT, () -> {
                batchCount = batchCountFunction.count(sourceNode.getConnectorContext(), null);
            }, "Batch count " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
        }

        StreamReadFunction streamReadFunction = sourceNode.getSourceFunctions().getStreamReadFunction();
        if (streamReadFunction != null) {
            Object recoveredOffset = null;
            if(streamOffsetStr != null) {
                recoveredOffset = JSON.parse(streamOffsetStr, Feature.SupportAutoType);
            }

            //TODO 提供方法返回增量断点， 不要使用wait的方式
            Object finalRecoveredOffset = recoveredOffset;
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
                streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events, offsetState, error, completed) -> {
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
                    if (error != null) {
                        PDKLogger.error(TAG, "Stream read occurred error {}, {}", error.getMessage(), LoggerUtils.sourceNodeMessage(sourceNode));
                    }
                    if (events != null) {
                        PDKLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                        offer(events);
                    }
                    if (offsetState != null) {
                        PDKLogger.debug(TAG, "Stream read update offset from {} to {}", this.streamOffsetStr, offsetState);
                        this.streamOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
                    }
                });
            }, "connect " + LoggerUtils.sourceNodeMessage(sourceNode), TAG, true, Long.MAX_VALUE, 5);
        }

        BatchReadFunction batchReadFunction = sourceNode.getSourceFunctions().getBatchReadFunction();
        if (batchReadFunction != null) {
            while (!batchCompleted) {
                Object recoveredOffset = null;
                if(batchOffsetStr != null) {
                    recoveredOffset = JSON.parse(batchOffsetStr, Feature.SupportAutoType);
                }
                Object finalRecoveredOffset = recoveredOffset;
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_READ,
                        () -> batchReadFunction.batchRead(sourceNode.getConnectorContext(), finalRecoveredOffset, (events, offsetState, error, completed) -> {
                            if (error != null) {
                                PDKLogger.error(TAG, "Batch read occurred error {} batchOffset {}, {}", error.getMessage(), batchOffsetStr, LoggerUtils.sourceNodeMessage(sourceNode));
                            }
                            if (events != null && !events.isEmpty()) {
                                PDKLogger.debug(TAG, "Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                                offer(events);

                                if (offsetState != null) {
                                    PDKLogger.debug(TAG, "Batch read update offset from {} to {}", this.batchOffsetStr, offsetState);
                                    batchOffsetStr = JSON.toJSONString(offsetState, SerializerFeature.WriteClassName);
                                }
                            }

                            if (!batchCompleted && completed) {
                                synchronized (streamLock) {
                                    if (!batchCompleted) {
                                        batchCompleted = true;
                                        PDKLogger.debug(TAG, "Batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                                        streamLock.notifyAll();
                                    }
                                }
                            }
                        }), "Batch read " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            }
        }

//        PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.TARGET_FUNCTIONS,
//                () -> targetNode.target.targetFunctions(targetNode.targetFunctions),
//                MessageFormat.format("call target functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), logger);
    }
}
