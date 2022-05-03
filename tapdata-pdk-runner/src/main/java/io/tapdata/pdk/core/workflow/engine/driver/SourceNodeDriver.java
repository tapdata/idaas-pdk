package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.control.TapForerunnerEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.common.InitFunction;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.ControlFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.pdk.core.workflow.engine.driver.task.TaskManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SourceNodeDriver extends Driver {
    private static final String TAG = SourceNodeDriver.class.getSimpleName();

    private SourceNode sourceNode;

    private String streamOffsetStr;
    private String batchOffsetStr;
    private String batchOffsetOnTable;
    private List<String> completedBatchTables = new ArrayList<>();
    private SourceStateListener sourceStateListener;

    private boolean enableBatchRead = true;
    private boolean enableStreamRead = true;
    private int batchLimit = 1000;
    private Long batchCount;
    private boolean batchCompleted = false;
    private final Object streamLock = new int[0];
    private final AtomicBoolean firstBatchRecordsOffered = new AtomicBoolean(false);
    private final AtomicBoolean shutDown = new AtomicBoolean(false);

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
    public static final int STATE_TABLE_PREPARED = 2;
    public static final int STATE_BATCH_STARTED = 10;
    public static final int STATE_BATCH_ENDED = 20;
    public static final int STATE_STREAM_STARTED = 30;
    public static final int STATE_STREAM_ENDED = 40;
    public static final int STATE_ENDED = 100;
    private KVMap<TapTable> tableKVMap;
    private List<String> finalTargetTables = new ArrayList<>();
    public void start() {
        start(null);
    }
    public void start(SourceStateListener sourceStateListener) {
        this.sourceStateListener = sourceStateListener;
        CommonUtils.ignoreAnyError(() -> {
            if(sourceStateListener != null)
                sourceStateListener.stateChanged(STATE_STARTED);
        }, TAG);
        TapLogger.info(TAG, "SourceNodeDriver started, {}", LoggerUtils.sourceNodeMessage(sourceNode));
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();

        KVMapFactory mapFactory = InstanceFactory.instance(KVMapFactory.class);
        tableKVMap = mapFactory.getCacheMap(sourceNode.getAssociateId(), TapTable.class);

        InitFunction initFunction = sourceNode.getConnectorFunctions().getInitFunction();
        if (initFunction != null) {
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.INIT, () -> {
                initFunction.init(sourceNode.getConnectorContext(), mapFactory.createKVReadOnlyMap(sourceNode.getAssociateId()));
            }, "Init " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
        }

        //Init tasks
        List<Map<String, Object>> tasks = sourceNode.getTasks();
        if(tasks != null && !tasks.isEmpty()) {
            taskManager = new TaskManager();
            taskManager.init(tasks);
        }

        //Fill the discovered table back into connector context
        //The table user input has to be one in the discovered tables. Otherwise we need create table logic which currently we don't have.
        List<String> targetTables = null;
        String nodeTable = sourceNode.getTable();
        List<String> nodeTables = sourceNode.getTables();
        if(nodeTables != null && !nodeTables.isEmpty()) {
            if(!nodeTables.get(0).equals("*")) {
                targetTables = new ArrayList<>(nodeTables);
            }
        }
        if(nodeTable != null) {
            if(targetTables == null)
                targetTables = new ArrayList<>(Collections.singletonList(nodeTable));
            else
                targetTables.add(nodeTable);
        }

        List<String> theFinalTargetTables = targetTables;
        pdkInvocationMonitor.invokePDKMethod(PDKMethod.DISCOVER_SCHEMA, () -> {
            sourceNode.getConnector().discoverSchema(sourceNode.getConnectorContext(), theFinalTargetTables, 10, (tableList) -> {
                if(tableList == null) return;
                List<TapEvent> forerunnerEvents = new ArrayList<>();
                TapForerunnerEvent lastOne = null;

                List<String> checkTableList = new ArrayList<>();
                if(theFinalTargetTables != null) {
                    checkTableList.addAll(theFinalTargetTables);
                }
                for(TapTable table : tableList) {
                    if(table == null) continue;
                    analyzeTableFields(table);
                    checkTableList.remove(table.getId());

                    if(taskManager != null) {
                        taskManager.filterTable(table, TAG);
                    }
                    lastOne = new TapForerunnerEvent().table(table).sampleRecords(null/* sample records here*/);
                    forerunnerEvents.add(lastOne);

                    tableKVMap.put(table.getId(), table);
                    finalTargetTables.add(table.getId());
                }
                if(!checkTableList.isEmpty()) {
                    throw new CoreException(ErrorCodes.SOURCE_TABLE_NOT_DISCOVERED, "Missing table(s) " + Arrays.toString(checkTableList.toArray()) + " after invoked discoverSchema method.");
                }
                if(!forerunnerEvents.isEmpty()) {
//                    lastOne.patrolListener(new PatrolListener() {
//                        @Override
//                        public void patrol(String nodeId, int state) {
//
//                        }
//                    });
                    //TODO shall wait forerunnerEvents complete in target node to start read other tables, to avoid OOM for large tables.
                    receivedExternalEvent(forerunnerEvents);
                }

            });
        }, "Discover schema " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);

        if(firstBatchRecordsOffered.compareAndSet(false, true)) {
            CommonUtils.ignoreAnyError(() -> {
                if(sourceStateListener != null)
                    sourceStateListener.stateChanged(STATE_TABLE_PREPARED);
            }, TAG);
        }

        BatchCountFunction batchCountFunction = sourceNode.getConnectorFunctions().getBatchCountFunction();
        if (enableBatchRead && batchCountFunction != null) {
            for(String table : finalTargetTables) {
                TapTable tapTable = tableKVMap.get(table);
                if(tapTable == null)
                    throw new CoreException(ErrorCodes.SOURCE_UNKNOWN_TABLE, "Unknown table " + table + " while batchCount");
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_COUNT, () -> {
                    //TODO batchOffset is not used yet.
                    batchCount += batchCountFunction.count(sourceNode.getConnectorContext(), tapTable, null);
                }, "Batch count " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            }
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
        if (enableBatchRead && batchReadFunction != null) {

            CommonUtils.ignoreAnyError(() -> {
                if(sourceStateListener != null)
                    sourceStateListener.stateChanged(STATE_BATCH_STARTED);
            }, TAG);
            for(String table : finalTargetTables) {
                if(completedBatchTables.contains(table)) {
                    continue;
                }
                TapTable tapTable = tableKVMap.get(table);
                if(tapTable == null)
                    throw new CoreException(ErrorCodes.SOURCE_UNKNOWN_TABLE, "Unknown table " + table + " while batchRead");
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_READ,
                        () -> batchReadFunction.batchRead(sourceNode.getConnectorContext(), tapTable, batchOffsetStr, batchLimit, (events, batchOffset) -> {
                            if (events != null && !events.isEmpty()) {
                                if(events.size() > batchLimit)
                                    throw new CoreException(ErrorCodes.SOURCE_EXCEEDED_BATCH_SIZE, "Batch read exceeded eventBatchSize " + batchLimit + " actual is " + events.size());
                                TapLogger.debug(TAG, "Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                                offerToQueue(events);

                                if(batchOffset != null){
                                    batchOffsetStr = batchOffset;
                                    batchOffsetOnTable = table;
                                }
//                            BatchOffsetFunction batchOffsetFunction = sourceNode.getConnectorFunctions().getBatchOffsetFunction();
//                            if(batchOffsetFunction != null) {
//                                pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_BATCH_OFFSET, () -> {
//                                    String offsetState = batchOffsetFunction.batchOffset(getSourceNode().getConnectorContext());
//                                    if(offsetState != null) {
//                                        TapLogger.debug(TAG, "Batch read update offset from {} to {}", this.batchOffsetStr, offsetState);
//                                        batchOffsetStr = offsetState;
//                                    }
//                                }, "Batch offset " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
//                            }
                            }
                        }), "Batch read " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
                batchOffsetStr = null;
                batchOffsetOnTable = null;
                if(!completedBatchTables.contains(table))
                    completedBatchTables.add(table);
            }
            if (!batchCompleted) {
                synchronized (streamLock) {
                    if (!batchCompleted) {
                        batchCompleted = true;
                        CommonUtils.ignoreAnyError(() -> {
                            if(sourceStateListener != null)
                                sourceStateListener.stateChanged(STATE_BATCH_ENDED);
                        }, TAG);
                        TapLogger.debug(TAG, "Batch read accomplished, {}", LoggerUtils.sourceNodeMessage(sourceNode));
                    }
                }
            }
        }

        StreamReadFunction streamReadFunction = sourceNode.getConnectorFunctions().getStreamReadFunction();
        if (enableStreamRead && streamReadFunction != null) {
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_STREAM_READ, () -> {
                while(!shutDown.get()) {
                    streamReadFunction.streamRead(sourceNode.getConnectorContext(), finalTargetTables, streamOffsetStr, batchLimit, StreamReadConsumer.create((events) -> {
                        if (events != null) {
                            if(events.size() > batchLimit)
                                throw new CoreException(ErrorCodes.SOURCE_EXCEEDED_BATCH_SIZE, "Batch read exceeded eventBatchSize " + batchLimit + " actual is " + events.size());
                            TapLogger.debug(TAG, "Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
                            offerToQueue(events);
                        }

                        StreamOffsetFunction streamOffsetFunction = sourceNode.getConnectorFunctions().getStreamOffsetFunction();
                        if(streamOffsetFunction != null) {
                            pdkInvocationMonitor.invokePDKMethod(PDKMethod.STREAM_OFFSET, () -> {
                                String offsetState = streamOffsetFunction.streamOffset(sourceNode.getConnectorContext(), finalTargetTables, null);
                                if (offsetState != null) {
                                    TapLogger.debug(TAG, "Stream read update offset from {} to {}", this.streamOffsetStr, offsetState);
                                    this.streamOffsetStr = offsetState;
                                }
                            }, "Stream read sourceNode " + sourceNode.getConnectorContext(), TAG, error -> {
                                TapLogger.error("streamOffset failed, {} sourceNode {}", error.getMessage(), sourceNode.getConnectorContext());
                            });
                        }
                    }).stateListener((from, to) -> {
                        if(to == StreamReadConsumer.STATE_STREAM_READ_STARTED) {
                            CommonUtils.ignoreAnyError(() -> {
                                if(sourceStateListener != null)
                                    sourceStateListener.stateChanged(STATE_STREAM_STARTED);
                            }, TAG);
                        } else if (to == StreamReadConsumer.STATE_STREAM_READ_ENDED){
                            CommonUtils.ignoreAnyError(() -> {
                                if(sourceStateListener != null)
                                    sourceStateListener.stateChanged(STATE_STREAM_ENDED);
                            }, TAG);
                        }

                    }));
                }
            }, "connect " + LoggerUtils.sourceNodeMessage(sourceNode), TAG, null, true, Long.MAX_VALUE, 5);
        }
    }

    private void offerToQueue(List<TapEvent> events) {
        offer(events, this::filterEvents);
    }

    public void receivedExternalEvent(List<TapEvent> events) {
        if(events == null)
            return;

        List<ControlEvent> controlEvents = new ArrayList<>();
//            targetNode.pullAllExternalEvents(tapEvent -> events.add(tapEvent));
        for (TapEvent event : events) {
            if(event instanceof ControlEvent) {
                controlEvents.add((ControlEvent) event);
            }/* else if(event instanceof TapBaseEvent) {
                ((TapBaseEvent) event).setTableName(sourceNode.getConnectorContext().getTable());
            }*/
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

        TapLogger.debug(TAG, "Handled {} of control events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourceNode));
        for(ControlEvent controlEvent : events) {
            if(controlEvent instanceof PatrolEvent) {
                handlePatrolEvent((PatrolEvent) controlEvent, PatrolEvent.STATE_ENTER);
            }

            if(controlFunction != null) {
                pdkInvocationMonitor.invokePDKMethod(PDKMethod.CONTROL, () -> {
                    controlFunction.control(sourceNode.getConnectorContext(), controlEvent);
                }, "control event " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
            }

            if(controlEvent instanceof PatrolEvent) {
                handlePatrolEvent((PatrolEvent) controlEvent, PatrolEvent.STATE_LEAVE);
            }
        }
    }

    private void handlePatrolEvent(PatrolEvent patrolEvent, int patrolState) {
        if(patrolEvent.applyState(sourceNode.getAssociateId(), patrolState)) {
            if(patrolEvent.getPatrolListener() != null) {
                CommonUtils.ignoreAnyError(() -> patrolEvent.getPatrolListener().patrol(sourceNode.getAssociateId(), patrolState), TAG);
            }
        }
    }

    @Override
    public void destroy() {
        CommonUtils.ignoreAnyError(() -> {
            if(sourceStateListener != null)
                sourceStateListener.stateChanged(STATE_ENDED);
            InstanceFactory.instance(KVMapFactory.class).reset(sourceNode.getAssociateId());
        }, TAG);
    }

    public List<TapEvent> filterEvents(List<TapEvent> events, boolean needClone) {
        TapCodecFilterManager codecFilterManager = sourceNode.getCodecsFilterManager();
        List<TapEvent> newEvents = new ArrayList<>();
        for(TapEvent tapEvent : events) {
            LinkedHashMap<String, TapField> nameFieldMap = null;
            if(tapEvent instanceof TapBaseEvent) {
                TapBaseEvent tapBaseEvent = (TapBaseEvent) tapEvent;
                TapTable table = tableKVMap.get(tapBaseEvent.getTableId());
                if(table == null)
                    throw new CoreException(ErrorCodes.SOURCE_UNKNOWN_TABLE, "Unknown table " + ((TapBaseEvent) tapEvent).getTableId() + " when send event " + tapEvent);
                nameFieldMap = table.getNameFieldMap();
                tapBaseEvent.setAssociateId(sourceNode.getAssociateId());
            }

            if(tapEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertDMLEvent = (TapInsertRecordEvent) tapEvent;
                TapInsertRecordEvent newInsertDMLEvent = null;
                if(needClone) {
                    newInsertDMLEvent = new TapInsertRecordEvent();
                    insertDMLEvent.clone(newInsertDMLEvent);
                } else {
                    newInsertDMLEvent = insertDMLEvent;
                }
                codecFilterManager.transformToTapValueMap(newInsertDMLEvent.getAfter(), nameFieldMap);
                newEvents.add(newInsertDMLEvent);
            } else if(tapEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent updateDMLEvent = (TapUpdateRecordEvent) tapEvent;
                TapUpdateRecordEvent newUpdateDMLEvent = null;
                if(needClone) {
                    newUpdateDMLEvent = new TapUpdateRecordEvent();
                    updateDMLEvent.clone(newUpdateDMLEvent);
                } else {
                    newUpdateDMLEvent = updateDMLEvent;
                }
                codecFilterManager.transformToTapValueMap(newUpdateDMLEvent.getAfter(), nameFieldMap);
                codecFilterManager.transformToTapValueMap(newUpdateDMLEvent.getBefore(), nameFieldMap);
                newEvents.add(newUpdateDMLEvent);
            } else if(tapEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteDMLEvent = (TapDeleteRecordEvent) tapEvent;
                TapDeleteRecordEvent newDeleteDMLEvent = null;
                if(needClone) {
                    newDeleteDMLEvent = new TapDeleteRecordEvent();
                    deleteDMLEvent.clone(newDeleteDMLEvent);
                } else {
                    newDeleteDMLEvent = deleteDMLEvent;
                }
                codecFilterManager.transformToTapValueMap(newDeleteDMLEvent.getBefore(), nameFieldMap);
                newEvents.add(newDeleteDMLEvent);
            } else {
                try {
                    TapEvent newTapEvent = tapEvent.getClass().getConstructor().newInstance();
                    tapEvent.clone(newTapEvent);
                    newEvents.add(newTapEvent);
                } catch (Throwable e) {
                    e.printStackTrace();
                    TapLogger.error(TAG, "New instance for {} failed, {}. TapEvent {} will be ignored", tapEvent.getClass(), e.getMessage(), tapEvent);
                }
            }
        }
        return newEvents;
    }

    public void analyzeTableFields(TapTable table) {
//        sourceNode.getConnectorContext().setTable(table);

        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if(nameFieldMap != null) {
            DefaultExpressionMatchingMap expressionMatchingMap = sourceNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap();
            TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
            if(tableFieldTypesGenerator == null)
                throw new CoreException(ErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");

            tableFieldTypesGenerator.autoFill(nameFieldMap, expressionMatchingMap);
        } else {
            //field data types is unknown, read 10 records to sample out the field data types
            table.setNameFieldMap(sampleRecords(table));
        }
    }

    private LinkedHashMap<String, TapField> sampleRecords(TapTable table) {
        final int sampleSize = 10;
        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = sourceNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
        if (queryByAdvanceFilterFunction != null) {
            PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
                    () -> queryByAdvanceFilterFunction.query(sourceNode.getConnectorContext(), TapAdvanceFilter.create().limit(sampleSize), (filterResults) -> {
                        if (filterResults != null && filterResults.getResults() != null) {
                            TapLogger.debug(TAG, "Batch read {} of events for sample field data types", filterResults.getResults().size());
                            fillNameFieldsFromSampleRecords(nameFieldMap, filterResults.getResults(), table.getDefaultPrimaryKeys());
                        }
                    }), "Batch read for sample data types " + LoggerUtils.sourceNodeMessage(sourceNode), TAG);
        }
        if(nameFieldMap.isEmpty()) {
            StringBuilder builder = new StringBuilder("Missing fields in table " + table.getName() + ". Please load field information for the table. ");
            if(queryByAdvanceFilterFunction == null)
                builder.append("Or implement queryByAdvanceFilterFunction for incremental engine to construct fields by sampling several records. ");
            else
                builder.append("Or provide initial records for incremental engine to construst fields by sample several records. ");
            throw new CoreException(ErrorCodes.SOURCE_MISSING_FIELDS_IN_TABLE, builder.toString());
        }
        return nameFieldMap;
    }

    private void fillNameFieldsFromSampleRecords(LinkedHashMap<String, TapField> nameFieldMap, List<Map<String, Object>> mapList, List<String> defaultPrimaryKeys) {
        TapCodecFilterManager codecFilterManager = sourceNode.getCodecsFilterManager();
        Map<String, ToTapValueCodec<?>> combinedMap = new LinkedHashMap<>();
        Map<String, Object> combinedValueMap = new HashMap<>();
        //Sample records
        for(Map<String, Object> value : mapList) {
            for(Map.Entry<String, Object> entry : value.entrySet()) {
                ToTapValueCodec<?> existingCodec = combinedMap.get(entry.getKey());

                if(entry.getValue() != null) {
                    ToTapValueCodec<?> newCodec = codecFilterManager.getToTapValueCodec(entry.getValue());
                    if(newCodec != null && existingCodec == null) {
                        combinedMap.put(entry.getKey(), newCodec);
                        combinedValueMap.put(entry.getKey(), entry.getValue());
                    } else if(newCodec != null) { // newCodec != null && existingCodec != null
                        if(!newCodec.equals(existingCodec)) {
                            TapLogger.error(TAG, "Found conflict field {} while sampling records, existing codec is {}, new codec is {}, the new codec will be ignored.", entry.getKey(), existingCodec.getClass().getSimpleName(), newCodec.getClass().getSimpleName());
                        }
                    }
                }
            }
        }

        //generate nameFieldMap
        int counter = 0;
        int primaryPos = 0;
        for(Map.Entry<String, ToTapValueCodec<?>> entry : combinedMap.entrySet()) {
            TapValue<?, ?> tapValue = entry.getValue().toTapValue(combinedValueMap.get(entry.getKey()));
            if(tapValue != null) {
                TapType tapType = tapValue.createDefaultTapType();
                TapField field = new TapField(entry.getKey(), tapType.getClass().getSimpleName()).tapType(tapType).pos(++counter);
                if(defaultPrimaryKeys != null && defaultPrimaryKeys.contains(entry.getKey())) {
                    field.isPrimaryKey(true).primaryKeyPos(++primaryPos);
                }
                nameFieldMap.put(entry.getKey(), field);
            }
        }
    }

    public int getBatchLimit() {
        return batchLimit;
    }

    public void setBatchLimit(int batchLimit) {
        this.batchLimit = batchLimit;
    }

    public boolean isEnableBatchRead() {
        return enableBatchRead;
    }

    public void setEnableBatchRead(boolean enableBatchRead) {
        this.enableBatchRead = enableBatchRead;
    }

    public boolean isEnableStreamRead() {
        return enableStreamRead;
    }

    public void setEnableStreamRead(boolean enableStreamRead) {
        this.enableStreamRead = enableStreamRead;
    }
}
