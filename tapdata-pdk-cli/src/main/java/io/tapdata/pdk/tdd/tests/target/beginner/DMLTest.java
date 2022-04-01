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
                dataFlowDescriber.setId("tdd->" + nodeInfo.getTapNodeSpecification().getId());
                String tableId = dataFlowDescriber.getId().replace('-', '_').replace('>', '_') + "_" + UUID.randomUUID().toString();
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
//        completed();
        waitCompleted(5);
    }

    private void insertOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap insertRecord = new DataMap();
        insertRecord.put("id", "id_2");
        insertRecord.put("tapString", "1234");
        insertRecord.put("tapString10", "0987654321");
        insertRecord.put("tapInt", 123123);
        insertRecord.put("tapBoolean", true);
        insertRecord.put("tapNumber", 1233);
        insertRecord.put("tapNumber52", 343.22);
        insertRecord.put("tapBinary", new byte[]{123, 21, 3, 2});

        DataMap filterMap = new DataMap();
        filterMap.put("id", "id_2");
        filterMap.put("tapString", "1234");
        sendInsertEvent(dataFlowEngine, dag, insertRecord, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                verifyBatchRecordExists(tddSourceNode, targetNode, filterMap);
                updateOneRecord(dataFlowEngine, dag);
            }
        }));
    }


    private void updateOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap after = new DataMap();
        DataMap before = new DataMap();
        before.put("id", "id_2");
        before.put("tapString", "1234");
        after.put("id", "id_2");
        after.put("tapString", "1234");
        after.put("tapInt", "5555");
        sendUpdateEvent(dataFlowEngine, dag, before, after, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                verifyUpdateOneRecord(targetNode, before, after);
                deleteOneRecord(dataFlowEngine, dag);
            }
        }));
    }

    private void deleteOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap before = new DataMap();
        before.put("id", "id_2");
        before.put("tapString", "1234");
        sendDeleteEvent(dataFlowEngine, dag, before, new PatrolEvent().patrolListener((nodeId, state) -> {
            if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE){
                verifyRecordNotExists(targetNode, before);
                completed();
            }
        }));
    }

    private void checkFunctions() {
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getWriteRecordFunction(), "WriteRecord is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly"));
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }
}
