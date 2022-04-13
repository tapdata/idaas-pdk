package io.tapdata.pdk.tdd.tests.target;

import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
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

                String tableId = testTableName(dataFlowDescriber.getId());

                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(sourceNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id(targetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(sourceNodeId, targetNodeId)));
                dataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)));

                dag = dataFlowDescriber.toDag();
                if (dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if (fromState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                            initConnectorFunctions();
                            checkFunctions(targetNode.getConnectorFunctions(), CreateTableTest.testFunctions());
                        } else if (toState.equals(DataFlowWorker.STATE_RECORDS_SENT)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_RECORDS_SENT", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    verifyTableFields();
                                    processCreateTable(dataFlowEngine, dag);

                                }
                            });
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
        });
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

    private void processCreateTable(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap insertRecord = buildInsertRecord();
        DataMap filterMap = buildFilterMap();

        sendInsertRecordEvent(dataFlowEngine, dag, insertRecord, new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyBatchRecordExists(tddSourceNode, targetNode, filterMap);
                sendDropTableEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                    if (innerNodeId.equals(targetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                        verifyRecordNotExists(targetNode,filterMap);
                        verifyTableNotExists(targetNode,filterMap);
                        sendCreateTableEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId1, innerState1) -> {
                            if (innerNodeId1.equals(targetNodeId) && innerState1 == PatrolEvent.STATE_LEAVE) {
                                processDML(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId2, innerState2) -> {
                                    if (innerNodeId2.equals(targetNodeId) && innerState2 == PatrolEvent.STATE_LEAVE) {
                                        sendDropTableEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId3, innerState3) -> {
                                            if(innerNodeId3.equals(targetNodeId) && innerState3 == PatrolEvent.STATE_LEAVE){
                                                completed();
                                            }
                                        }));
                                    }
                                }));
                            }
                        }));
                    }
                }));
            }
        }));
    }

    private void processDML(DataFlowEngine dataFlowEngine, TapDAG dag, PatrolEvent patrolEvent) {
        DataMap insertRecord = buildInsertRecord();
        DataMap filterMap = buildFilterMap();
        DataMap updateMap = buildUpdateMap();

        sendInsertRecordEvent(dataFlowEngine, dag, insertRecord, new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyBatchRecordExists(tddSourceNode, targetNode, filterMap);
                sendUpdateRecordEvent(dataFlowEngine, dag, filterMap, updateMap, new PatrolEvent().patrolListener((innerNodeId1, innerState1) -> {
                    if (innerNodeId1.equals(targetNodeId) && innerState1 == PatrolEvent.STATE_LEAVE) {
                        verifyUpdateOneRecord(targetNode, filterMap, updateMap);
                        sendDeleteRecordEvent(dataFlowEngine, dag, filterMap, new PatrolEvent().patrolListener((innerNodeId2, innerState2) -> {
                            if (innerNodeId2.equals(targetNodeId) && innerState2 == PatrolEvent.STATE_LEAVE) {
                                verifyRecordNotExists(targetNode, filterMap);
                                dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
                            }
                        }));
                    }
                }));
            }
        }));
    }

    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to implement a Target, please implement it in registerCapabilities method."),
                support(QueryByFilterFunction.class, "QueryByFilter is needed for TDD to verify the record is written correctly, please implement it in registerCapabilities method."),
                support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method."),
                support(CreateTableFunction.class, "CreateTable is needed for database who need create table before insert records, please implement it in registerCapabilities method.")
        );
    }

//    private void checkFunctions() {
//        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
//        $(() -> Assertions.assertNotNull(connectorFunctions.getWriteRecordFunction(), "WriteRecord is a must to implement a Target"));
//        $(() -> Assertions.assertNotNull(connectorFunctions.getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly"));
//        $(() -> Assertions.assertNotNull(connectorFunctions.getCreateTableFunction(), "CreateTable is needed for database who need create table before insert records"));
////        $(() -> Assertions.assertNotNull(connectorFunctions.getBatchCountFunction(), "BatchCount is needed for verify how many records have inserted"));
//    }

    private void initConnectorFunctions() {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }


}
