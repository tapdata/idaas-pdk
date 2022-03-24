package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.pretty.ClassHandlers;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.pdk.core.utils.queue.ListHandler;
import io.tapdata.pdk.core.workflow.engine.JobOptions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TargetNodeDriver implements ListHandler<List<TapEvent>> {
    private static final String TAG = TargetNodeDriver.class.getSimpleName();

    private TargetNode targetNode;

    private List<String> actionsBeforeStart;

    private AtomicBoolean started = new AtomicBoolean(false);

    private AtomicBoolean firstNonControlReceived = new AtomicBoolean(false);

    private ClassHandlers classHandlers = new ClassHandlers();
    public TargetNodeDriver() {
        classHandlers.register(TapCreateTableEvent.class, this::handleCreateTableEvent);
        classHandlers.register(TapAlterTableEvent.class, this::handleAlterTableEvent);
        classHandlers.register(TapClearTableEvent.class, this::handleClearTableEvent);
        classHandlers.register(TapDropTableEvent.class, this::handleDropTableEvent);

        classHandlers.register(TapInsertRecordEvent.class, this::filterInsertEvent);
        classHandlers.register(TapUpdateRecordEvent.class, this::filterUpdateEvent);
        classHandlers.register(TapDeleteRecordEvent.class, this::filterDeleteEvent);
    }

    private void handleCreateTableEvent(TapCreateTableEvent createTableEvent) {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        CreateTableFunction createTableFunction = targetNode.getConnectorFunctions().getCreateTableFunction();
        if(createTableFunction != null) {
            PDKLogger.debug(TAG, "Create table {} before start. {}", targetNode.getConnectorContext().getTable(), LoggerUtils.targetNodeMessage(targetNode));


            pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_CREATE_TABLE, () -> {
                createTableFunction.createTable(getTargetNode().getConnectorContext(), createTableEvent);
            }, "Clear table " + LoggerUtils.targetNodeMessage(targetNode), TAG);
        }
    }

    private void handleAlterTableEvent(TapAlterTableEvent alterTableEvent) {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        AlterTableFunction alterTableFunction = targetNode.getConnectorFunctions().getAlterTableFunction();
        if(alterTableFunction != null) {
            PDKLogger.debug(TAG, "Alter table {} before start. {}", targetNode.getConnectorContext().getTable(), LoggerUtils.targetNodeMessage(targetNode));
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_ALTER_TABLE, () -> {
                alterTableFunction.alterTable(getTargetNode().getConnectorContext(), alterTableEvent);
            }, "Alter table " + LoggerUtils.targetNodeMessage(targetNode), TAG);
        }
    }

    private void handleClearTableEvent(TapClearTableEvent clearTableEvent) {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        ClearTableFunction clearTableFunction = targetNode.getConnectorFunctions().getClearTableFunction();
        if(clearTableFunction != null) {
            PDKLogger.debug(TAG, "Clear table {} before start. {}", targetNode.getConnectorContext().getTable(), LoggerUtils.targetNodeMessage(targetNode));
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_CLEAR_TABLE, () -> {
                clearTableFunction.clearTable(getTargetNode().getConnectorContext(), clearTableEvent);
            }, "Clear table " + LoggerUtils.targetNodeMessage(targetNode), TAG);
        }
    }

    private void handleDropTableEvent(TapDropTableEvent dropTableEvent) {
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        DropTableFunction dropTableFunction = targetNode.getConnectorFunctions().getDropTableFunction();
        if(dropTableFunction != null) {
            PDKLogger.debug(TAG, "Drop table {} before start. {}", targetNode.getConnectorContext().getTable(), LoggerUtils.targetNodeMessage(targetNode));
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_DROP_TABLE, () -> {
                dropTableFunction.dropTable(getTargetNode().getConnectorContext(), dropTableEvent);
                //clear the index and fields
                TapTable table = targetNode.getConnectorContext().getTable();
                if(table != null) {
                    table.setIndexList(null);
                    table.setNameFieldMap(null);
                }
            }, "Drop table " + LoggerUtils.targetNodeMessage(targetNode), TAG);
        }
    }


    @Override
    public void execute(List<List<TapEvent>> list) throws Throwable {
//        if(started.compareAndSet(false, true)) {
//            replaceTableFromDiscovered();
//        }

//        if(connected.compareAndSet(false, true)) {
//            ConnectFunction connectFunction = targetNode.getTargetFunctions().getConnectFunction();
//            if(connectFunction != null) {
//                pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_CONNECT, () -> {
//                    connectFunction.connect(targetNode.getConnectorContext());
//                }, "connect " + LoggerUtils.targetNodeMessage(targetNode), logger);
//            }
//        }

        List<TapRecordEvent> recordEvents = new ArrayList<>();
        for(List<TapEvent> events : list) {
            for (TapEvent event : events) {
                if(!firstNonControlReceived.get() && event instanceof TapBaseEvent) {
                    TapTable table = ((TapBaseEvent) event).getTable();
                    firstNonControlReceived.set(true);
                    handleActionsBeforeStart(table);

                    tableInitialCheck(table);
                }
                if(event instanceof TapDDLEvent) {
                    //force to handle DML before handle DDL.
                    handleRecordEvents(recordEvents);
                    //handle ddl events
                    handleDDLEvent((TapDDLEvent) event);
                }
                if(event instanceof TapRecordEvent) {
                    recordEvents.add(filterEvent((TapRecordEvent) event));
                }
            }
        }
        handleRecordEvents(recordEvents);

    }

    private void tableInitialCheck(TapTable incomingTable) {
        TapTable targetTable = targetNode.getConnectorContext().getTable();
        LinkedHashMap<String, TapField> targetFieldMap = targetTable.getNameFieldMap();
        if(targetFieldMap == null || targetFieldMap.isEmpty()) {
            return;
        }

        LinkedHashMap<String, TapField> incomingTableFieldMap = incomingTable.getNameFieldMap();
        if(incomingTableFieldMap == null) {
            return;
        }

        StringBuilder builder = new StringBuilder();
        boolean somethingWrong = false;
        for(Map.Entry<String, TapField> entry : incomingTableFieldMap.entrySet()) {
            TapField targetTapField = targetFieldMap.get(entry.getKey());
            if(targetTapField == null) {
                builder.append("field ").append(entry.getKey()).append(" not found; ");
                if(!somethingWrong) somethingWrong = true;
            } else {
                if(targetTapField.getTapType() != null && entry.getValue().getTapType() != null) {
                    if(!targetTapField.getTapType().getClass().equals(entry.getValue().getTapType().getClass())) {
                        builder.append("field ").append(entry.getKey()).append(" tapType doesn't match, source ").append(entry.getValue().getClass()).append(" expect ").append(targetTapField.getTapType().getClass()).append("; ");
                        if(!somethingWrong) somethingWrong = true;
                    }
                } else {
                    builder.append("field ").append(entry.getKey()).append(" tapType doesn't match (null), source ").append(entry.getValue()).append(" expect ").append(targetTapField.getTapType()).append("; ");
                    if(!somethingWrong) somethingWrong = true;
                }
            }
        }
        if(somethingWrong) {
            PDKLogger.warn(TAG, "Verify table fields failed, {}, {}", builder.toString(), LoggerUtils.targetNodeMessage(targetNode));
        }
    }

    private void handleActionsBeforeStart(TapTable table) {
        configTable(table);

        if(actionsBeforeStart != null) {
            for(String action : actionsBeforeStart) {
                switch (action) {
                    case JobOptions.ACTION_DROP_TABLE:
                        final TapDropTableEvent dropTableEvent = newTableEvent(TapDropTableEvent.class);
                        if(dropTableEvent != null)
                            classHandlers.handle(dropTableEvent);
                        break;
                    case JobOptions.ACTION_CLEAR_TABLE:
                        final TapClearTableEvent clearTableEvent = newTableEvent(TapClearTableEvent.class);
                        if(clearTableEvent != null)
                            classHandlers.handle(clearTableEvent);
                        break;
                    case JobOptions.ACTION_CREATE_TABLE:
                        final TapCreateTableEvent createTableEvent = newTableEvent(TapCreateTableEvent.class);
                        if(createTableEvent != null)
                            classHandlers.handle(createTableEvent);
                        break;
                    case JobOptions.ACTION_INDEX_PRIMARY:
                        break;
                    default:
                        PDKLogger.error(TAG, "Action {} is unknown before start, {}", action, LoggerUtils.targetNodeMessage(targetNode));
                        break;
                }
            }
        }
    }

    private void configTable(TapTable sourceTable) {
        TapTable targetTable = targetNode.getConnectorContext().getTable();
        //Convert source table to target target by calculate the originType of target database.
        TargetTypesGenerator targetTypesGenerator = new TargetTypesGenerator();
        LinkedHashMap<String, TapField> nameFieldMap = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), targetNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap());
        targetTable.setNameFieldMap(nameFieldMap);
    }

    private void replaceTableFromDiscovered() {
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

    private void handleDDLEvent(TapDDLEvent event) {
        classHandlers.handle(event);
    }

    private void handleRecordEvents(List<TapRecordEvent> recordEvents) {
        if(recordEvents.isEmpty())
            return;
        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
        WriteRecordFunction insertRecordFunction = targetNode.getConnectorFunctions().getDmlFunction();
        if(insertRecordFunction != null) {
            PDKLogger.debug(TAG, "Handled {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
            pdkInvocationMonitor.invokePDKMethod(PDKMethod.TARGET_DML, () -> {
                insertRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents, (event) -> {
                    PDKLogger.debug(TAG, "Handled {} of record events, {}", recordEvents.size(), LoggerUtils.targetNodeMessage(targetNode));
                });
            }, "insert " + LoggerUtils.targetNodeMessage(targetNode), TAG);
        }
        recordEvents.clear();
    }

    private <T extends TapTableEvent> T newTableEvent(Class<T> tableEventClass) {
        try {
            T t = tableEventClass.getConstructor().newInstance();
            t.setTable(targetNode.getConnectorContext().getTable());
            t.setTime(System.currentTimeMillis());
            t.setPdkId(targetNode.getTapNodeInfo().getTapNodeSpecification().getId());
            t.setPdkGroup(targetNode.getTapNodeInfo().getTapNodeSpecification().getGroup());
            t.setPdkVersion(targetNode.getTapNodeInfo().getTapNodeSpecification().getVersion());
            return t;
        } catch (Throwable e) {
            e.printStackTrace();
            PDKLogger.error(TAG, "Create table event {} failed, {}", tableEventClass, e.getMessage());
        }
        return null;
    }

    public TargetNode getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(TargetNode targetNode) {
        this.targetNode = targetNode;
    }

    private TapRecordEvent filterEvent(TapRecordEvent recordEvent) {
        classHandlers.handle(recordEvent);
//        if(recordEvent instanceof TapInsertRecordEvent) {
//            filterInsertEvent((TapInsertRecordEvent) recordEvent);
//        } else if(recordEvent instanceof TapUpdateRecordEvent) {
//            filterUpdateEvent((TapUpdateRecordEvent) recordEvent);
//        } else if(recordEvent instanceof TapDeleteRecordEvent) {
//            filterDeleteEvent((TapDeleteRecordEvent) recordEvent);
//        }
        return recordEvent;
    }

    private List<TapEvent> filterEvents(List<TapEvent> events) {
        for(TapEvent tapEvent : events) {
            classHandlers.handle(tapEvent);
//            if(tapEvent instanceof TapInsertRecordEvent) {
//                filterInsertEvent((TapInsertRecordEvent) tapEvent);
//            } else if(tapEvent instanceof TapUpdateRecordEvent) {
//                filterUpdateEvent((TapUpdateRecordEvent) tapEvent);
//            } else if(tapEvent instanceof TapDeleteRecordEvent) {
//                filterDeleteEvent((TapDeleteRecordEvent) tapEvent);
//            }
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

    public void setActionsBeforeStart(List<String> actionsBeforeStart) {
        this.actionsBeforeStart = actionsBeforeStart;
    }
}
