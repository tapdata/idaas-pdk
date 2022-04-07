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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

@DisplayName("Tests for source beginner test")
public class ReadTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode tddTargetNode;
    SourceNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String sourceNodeId = "s1";

    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();

                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId("tdd->" + nodeInfo.getTapNodeSpecification().getId());
                String tableId = dataFlowDescriber.getId().replace('-', '_').replace('>', '_') + "_" + UUID.randomUUID().toString();
                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(sourceNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions),
                        new TapDAGNodeEx().id(targetNodeId).pdkId("tdd-target").group("io.tapdata.connector").type(TapDAGNode.TYPE_TARGET).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap())
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList("s1", "t2")));
                dataFlowDescriber.setJobOptions(new JobOptions());

                dag = dataFlowDescriber.toDag();
                if (dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                            initConnectorFunctions(nodeInfo, tableId, dataFlowDescriber.getId());
                            checkFunctions();
                        } else if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    completed();
                                }
                            });
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


    private void checkFunctions() {
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchReadFunction(), "BatchReadFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchCountFunction(), "BatchCountFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(sourceNode.getConnectorFunctions().getBatchOffsetFunction(), "BatchOffsetFunction is a must to implement a Target"));
//        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getStreamReadFunction(), "StreamReadFunction is a must to implement a Target"));
//        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getStreamOffsetFunction(), "StreamOffsetFunction is a must to implement a Target"));
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }
}
