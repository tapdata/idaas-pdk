package io.tapdata.pdk.tdd.tests.target.beginner;


import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
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

@DisplayName("Tests for target beginner test")
public class DMLTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode targetNode;
    SourceNode tddSourceNode;
    PatrolEvent firstPatrolEvent;
    DataMap firstRecord;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String sourceNodeId = "s1";

    @Test
    @DisplayName("Test method handleDML")
    void targetTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();

                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId("tddTo" + nodeInfo.getTapNodeSpecification().getId());
                String tableId = testTableName(dataFlowDescriber.getId());
                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(sourceNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id(targetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList("s1", "t2")));
                dataFlowDescriber.setJobOptions(new JobOptions());

                dag = dataFlowDescriber.toDag();
                if(dag != null){
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if(toState.equals(DataFlowWorker.STATE_INITIALIZING)){
                            initConnectorFunctions(nodeInfo, tableId, dataFlowDescriber.getId());

                            checkFunctions();
                        } else if(toState.equals(DataFlowWorker.STATE_INITIALIZED)){
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                                    insertOneRecord(dataFlowEngine, dag);
                                }
                            });
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
                        }
                    });
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                CommonUtils.logError(TAG, "Start failed", throwable);
                if(throwable instanceof AssertionFailedError){
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

    private void insertOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap insertRecord = buildInsertRecord();
        DataMap filterMap =buildFilterMap();
        sendInsertRecordEvent(dataFlowEngine, dag, insertRecord, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                verifyBatchRecordExists(tddSourceNode, targetNode, filterMap);
                updateOneRecord(dataFlowEngine, dag);
            }
        }));
    }


    private void updateOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap updateMap = buildUpdateMap();
        DataMap filterMap = buildFilterMap();
        sendUpdateRecordEvent(dataFlowEngine, dag, filterMap, updateMap, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                verifyUpdateOneRecord(targetNode, filterMap, updateMap);
                deleteOneRecord(dataFlowEngine, dag);
            }
        }));
    }

    private void deleteOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap filterMap = buildFilterMap();
        sendDeleteRecordEvent(dataFlowEngine, dag, filterMap, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyRecordNotExists(targetNode, filterMap);
                sendDropTableEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                    if (innerNodeId.equals(targetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                        consumeQualifiedTapNodeInfo(nodeInfo -> {
                            Assertions.assertNotNull(connectionOptions, "Missing \"connection\" key in test config json file. The value of \"connection\" key is the user input values of json form items. ");

                            prepareConnectionNode(nodeInfo, connectionOptions, connectionNode -> {
                                List<TapTable> allTables = new ArrayList<>();
                                try {
                                    connectionNode.discoverSchema(tables -> allTables.addAll(tables));
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                    Assertions.fail(throwable);
                                }
                                TapTable targetTable = dag.getNodeMap().get(targetNodeId).getTable();
                                for(TapTable table : allTables) {
                                    if(table.getName() != null && table.getName().equals(targetTable.getName())) {
                                        $(() -> Assertions.fail("Target table " + targetTable.getName() + " should be deleted, because dropTable has been called, please check your dropTable method whether it works as expected or not"));
                                    }
                                }
                                connectionNode.getConnectorNode().destroy();
                                completed();
                            });
                        });
                    }
                }));
            }
        }));
    }

    private void checkFunctions() {
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getWriteRecordFunction(), "WriteRecord is a must to implement a Target, please implement it in registerCapabilities method."));
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly, please implement it in registerCapabilities method."));
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getDropTableFunction(), "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method."));
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }
}
