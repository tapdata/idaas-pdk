package io.tapdata.pdk.tdd.tests.target.beginner;


import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.state.StateListener;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.function.Consumer;

@DisplayName("Tests for target beginner test")
public class DMLTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode targetNode;

    @Test
    @DisplayName("Test method handleDML")
    void targetTest() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
                dataFlowEngine.start();

                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId("tdd->" + nodeInfo.getTapNodeSpecification().getId());

                String tableId = dataFlowDescriber.getId() + "_" + UUID.randomUUID().toString();

                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id("s1").pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id("t2").pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList("s1", "t2")));
                dataFlowDescriber.setJobOptions(new JobOptions());

                initConnectorFunctions(nodeInfo, tableId, dataFlowDescriber.getId());

                checkFunctions();

                verifyBatchRecordNotExist();

                TapDAG dag = dataFlowDescriber.toDag();
                if(dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if(toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if(nodeId.equals("t2") && state == PatrolEvent.STATE_LEAVE) {

                                }
                            });
                            patrolEvent.addInfo("tdd", "aaa");
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
                        }
                    });
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                CommonUtils.logError(TAG, "Start failed", throwable);
                if(throwable instanceof AssertionFailedError) {
                    throw ((AssertionFailedError) throwable);
                } else {
                    Assertions.fail("Unknown error " + throwable.getMessage());
                }
            }
        }, TapNodeInfo.NODE_TYPE_TARGET, TapNodeInfo.NODE_TYPE_SOURCE_TARGET);
    }

    private void verifyBatchRecordNotExist() throws Throwable {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        DataMap match = new DataMap();
        match.put("id", "id_1");
        match.put("tapString", "123");
        TapFilter filter = new TapFilter();
        filter.setMatch(match);
        List<TapFilter> filters = Arrays.asList(filter);
        queryByFilterFunction.query(targetNode.getConnectorContext(), filters, filterResults -> {
            Assertions.assertFalse(filterResults.isEmpty(), "There is one filter for queryByFilter, then filterResults size has to be 1");
            FilterResult filterResult = filterResults.get(0);
            Assertions.assertNull(filterResult.getError(), "Should be no value, error should not be threw");
            Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record hasn't be inserted yet");
        });
    }

    private void checkFunctions() {
        Assertions.assertNotNull(targetNode.getConnectorFunctions().getWriteRecordFunction(), "DMLFunction is a must to implement a Target");
        Assertions.assertNotNull(targetNode.getConnectorFunctions().getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly");
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        targetNode = PDKIntegration.createTargetBuilder()
                .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                .withAssociateId("tdd_associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                .withTable(new TapTable(tableId))
                .withDagId(dagId)
                .withConnectionConfig(connectionOptions)
                .build();

        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecRegistry codecRegistry = new TapCodecRegistry();
        targetNode.getConnector().registerCapabilities(connectorFunctions, codecRegistry);
    }
}
