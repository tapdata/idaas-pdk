package io.tapdata.pdk.tdd.tests.target.intermediate;

import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.*;

@DisplayName("Tests for target intermediate test")
public class CreateTableTest extends PDKTestBase {
    private static final String TAG = CreateTableTest.class.getSimpleName();
    TargetNode targetNode;
    SourceNode tddSourceNode;
    PatrolEvent firstPatrolEvent;
    Map<String, Object> firstRecord;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String sourceNodeId = "s1";

    @Test
    @DisplayName("Test method createTable")
    void createTableTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
//                dataFlowEngine.start();

                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId("createTableTest->" + nodeInfo.getTapNodeSpecification().getId());

                String tableId = dataFlowDescriber.getId() + "_" + UUID.randomUUID();
                tableId = tableId.replace('-', '_').replace('>','_');

                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(sourceNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id(targetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList("s1", "t2")));
                dataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)));

                dag = dataFlowDescriber.toDag();
                if (dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                            initConnectorFunctions();
                            checkFunctions();
//                            tddSourceNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap();
                        } else if (toState.equals(DataFlowWorker.STATE_RECORDS_SENT)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_RECORDS_SENT", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    verifyTableFields();
                                    insertOneRecord(dataFlowEngine, dag);

                                }
                            });
//                            patrolEvent.addInfo("tdd", "aaa");
                            //Line up after batch read
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
                        }
                    });
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                CommonUtils.logError(TAG, "Start failed", throwable);
                if (throwable instanceof AssertionFailedError) {
                    $(() -> {
                        throw ((AssertionFailedError) throwable);
                    });
                } else {
                    $(() -> Assertions.fail("Unknown error " + throwable.getMessage()));
                }
            }
        }, TapNodeInfo.NODE_TYPE_TARGET, TapNodeInfo.NODE_TYPE_SOURCE_TARGET);
        waitCompleted(50);
    }

    private void verifyTableFields() {
        TapTable table = targetNode.getConnectorContext().getTable();
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        $(() -> Assertions.assertNotNull(nameFieldMap, "Table name fields is null, please check whether you provide \"dataTypes\" in spec json file or define from TapValue codec in registerCapabilities method"));

        boolean missingOriginType = false;
        StringBuilder builder = new StringBuilder("Missing originType for fields, \n");
        for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            if (entry.getValue().getOriginType() == null) {
                missingOriginType = true;
                builder.append("\t").append("Field \"").append(entry.getKey()).append("\" missing originType for TapType \"").append(entry.getValue().getTapType().getClass().getSimpleName()).append("\"\n");
            }
        }
        builder.append("You may register your codec for unsupported TapValue in registerCapabilities. For example, codecRegistry.registerFromTapValue(TapRawValue.class, \"TEXT\"), this is register unsupported TapRawValue to supported TEXT and please provide the conversion method. ");
        boolean finalMissingOriginType = missingOriginType;
        $(() -> Assertions.assertFalse(finalMissingOriginType, builder.toString()));
    }

    private void insertOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        firstRecord = new DataMap();
        firstRecord.put("id", "id_2");
        firstRecord.put("tapString", "1234");
        firstRecord.put("tapString10", "0987654321");
        firstRecord.put("tapInt", 123123);
        firstRecord.put("tapBoolean", true);
        firstRecord.put("tapNumber", 1233);
        firstRecord.put("tapNumber52", 343.22);
        firstRecord.put("tapBinary", new byte[]{123, 21, 3, 2});
        insertRecordEvent.setAfter(firstRecord);

        dataFlowEngine.sendExternalTapEvent(dag.getId(), insertRecordEvent);
        PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyBatchRecordExists();

                // send drop table event
                TapDropTableEvent tapDropTableEvent = new TapDropTableEvent();
                dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDropTableEvent);

                PatrolEvent dropTablePatrolEvent = new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                    PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", innerNodeId, (innerState == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                    if (innerNodeId.equals(targetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                        // send create table event
                        TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
                        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapCreateTableEvent);
                        PatrolEvent createTablePatrolEvent = new PatrolEvent().patrolListener((innerNodeId1, innerState1) -> {
                            PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", innerNodeId1, (innerState1 == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                            if (innerNodeId1.equals(targetNodeId) && innerState1 == PatrolEvent.STATE_LEAVE) {
                                verifyRecordNotExists();
                                // send drop table event
                                dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDropTableEvent);
                                PatrolEvent dropTablePatrolEvent2 = new PatrolEvent().patrolListener((innerNodeId2, innerState2) -> {
                                    completed();
                                });
                                dataFlowEngine.sendExternalTapEvent(dag.getId(),dropTablePatrolEvent2);
                            }
                        });
                        dataFlowEngine.sendExternalTapEvent(dag.getId(), createTablePatrolEvent);
                    }
                });
                dataFlowEngine.sendExternalTapEvent(dag.getId(), dropTablePatrolEvent);


            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }
    private void verifyBatchRecordExists() {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        DataMap match = new DataMap();
        match.put("id", "id_2");
        match.put("tapString", "1234");
        TapFilter filter = new TapFilter();
        filter.setMatch(match);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(match) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNull(filterResult.getError(), "Error occurred while queryByFilter " + InstanceFactory.instance(JsonParser.class).toJson(match) + " error " + filterResult.getError()));
        $(() -> Assertions.assertNotNull(filterResult.getResult(), "Result should not be null, as the record has been inserted"));
        Map<String, Object> result = filterResult.getResult();


        tddSourceNode.getCodecFilterManager().transformToTapValueMap(firstRecord, tddSourceNode.getConnectorContext().getTable().getNameFieldMap());
        tddSourceNode.getCodecFilterManager().transformFromTapValueMap(firstRecord);

        StringBuilder builder = new StringBuilder();
        $(() -> Assertions.assertFalse(mapEquals(firstRecord, result, builder), builder.toString()));
    }


    private void verifyRecordNotExists() {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        DataMap match = new DataMap();
        match.put("id", "id_2");
        match.put("tapString", "1234");
        TapFilter filter = new TapFilter();
        filter.setMatch(match);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(match) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNull(filterResult.getError(), "Should be no value, error should not be threw"));
        $(() -> Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record has been deleted, please make sure TapDeleteRecordEvent is handled well in writeRecord method."));
    }

    private void checkFunctions() {
        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
        $(() -> Assertions.assertNotNull(connectorFunctions.getWriteRecordFunction(), "WriteRecord is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(connectorFunctions.getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly"));
        $(() -> Assertions.assertNotNull(connectorFunctions.getCreateTableFunction(), "CreateTable is needed for database who need create table before insert records"));
//        $(() -> Assertions.assertNotNull(connectorFunctions.getBatchCountFunction(), "BatchCount is needed for verify how many records have inserted"));
    }

    private void initConnectorFunctions() {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }


}
