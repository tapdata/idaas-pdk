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
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@DisplayName("Tests for source beginner test")
public class ReadTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode tddTargetNode;
    SourceNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testTargetNodeId = "tt1";
    String testSourceNodeId = "ts1";
    String originNodeId = "r0";

    DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
    String sourceToTargetId;
    String originToSourceId;
    TapDAG originDag;

    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                sourceToTargetId = nodeInfo.getTapNodeSpecification().getId() + " ->tdd target";
                originToSourceId = "origin-> " + nodeInfo.getTapNodeSpecification().getId();


                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                // #1
                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId(sourceToTargetId);
                String tableId = dataFlowDescriber.getId().replace('-', '_').replace('>', '_') + "_" + UUID.randomUUID().toString();


                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(testSourceNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions),
                        new TapDAGNodeEx().id(targetNodeId).pdkId("tdd-target").group("io.tapdata.connector").type(TapDAGNode.TYPE_TARGET).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap())
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(testSourceNodeId, targetNodeId)));
                dataFlowDescriber.setJobOptions(new JobOptions());
                dag = dataFlowDescriber.toDag();

                // #2
                DAGDescriber originDataFlowDescriber = new DAGDescriber();
                originDataFlowDescriber.setId(originToSourceId);

                originDataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(originNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id(testTargetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                originDataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(originNodeId, testTargetNodeId)));
                originDataFlowDescriber.setJobOptions(new JobOptions());

                originDag = originDataFlowDescriber.toDag();

                if (dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                            initConnectorFunctions();
                            checkFunctions();
                        } else if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    processStreamInsert();
                                }
                            });
                            dataFlowEngine.sendExternalTapEvent(sourceToTargetId, patrolEvent);
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
        waitCompleted(5000000);
    }

    private void processStreamInsert() {
        dataFlowEngine.startDataFlow(originDag, new JobOptions(), (fromState, toState, dataFlowWorker) -> {
            if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                    PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                    if (nodeId.equals(testTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                        PatrolEvent streamPatrolEvent = new PatrolEvent();
                        streamPatrolEvent.addInfo("streamRead", true);
                        dataFlowEngine.sendExternalTapEvent(sourceToTargetId,streamPatrolEvent);
                        AtomicInteger cnt = new AtomicInteger();
                        for (int i = 0; i < 10; i++) {
                            DataMap dataMap = buildInsertRecord();
                            dataMap.put("id", "id_" + i);
                            sendInsertRecordEvent(dataFlowEngine, originDag, dataMap, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                                if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                                    cnt.addAndGet(1);
                                }
                            }));
                        }
                        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
                            PatrolEvent innerPatrolEvent = new PatrolEvent();
                            innerPatrolEvent.addInfo("callback", (Consumer<Integer>) streamCount -> {
                                Assertions.assertEquals(cnt.get(), streamCount);
                                completed();
                            });
                            dataFlowEngine.sendExternalTapEvent(sourceToTargetId, innerPatrolEvent);
                        }, 5, TimeUnit.SECONDS);

                    }
                });
                dataFlowEngine.sendExternalTapEvent(originToSourceId, patrolEvent);
            }
        });

    }


    private void checkFunctions() {
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchReadFunction(), "BatchReadFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchCountFunction(), "BatchCountFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchOffsetFunction(), "BatchOffsetFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getStreamReadFunction(), "StreamReadFunction is a must to implement a Target"));
//        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getStreamOffsetFunction(), "StreamOffsetFunction is a must to implement a Target"));
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }
}
