package io.tapdata.pdk.tdd.tests.target.beginner;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.control.PatrolEvent;
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
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
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

@DisplayName("Tests for target beginner test")
public class DMLTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode targetNode;
    SourceNode tddSourceNode;
    PatrolEvent firstPatrolEvent;
    Map<String, Object> firstRecord ;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String sourceNodeId = "s1";

    @Test
    @DisplayName("Test method handleDML")
    void targetTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            try {
                DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
                dataFlowEngine.start();

                DAGDescriber dataFlowDescriber = new DAGDescriber();
                dataFlowDescriber.setId("tdd->" + nodeInfo.getTapNodeSpecification().getId());

                String tableId = dataFlowDescriber.getId() + "_" + UUID.randomUUID().toString();

                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                dataFlowDescriber.setNodes(Arrays.asList(
                        new TapDAGNodeEx().id(sourceNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                                table(new TapTable("tdd-table")).connectionConfig(new DataMap()),
                        new TapDAGNodeEx().id(targetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(nodeInfo.getNodeType()).version(spec.getVersion()).
                                table(new TapTable(tableId)).connectionConfig(connectionOptions)
                ));
                dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList("s1", "t2")));
                dataFlowDescriber.setJobOptions(new JobOptions());

                TapDAG dag = dataFlowDescriber.toDag();
                if(dag != null) {
                    JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                    dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                        if(toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                            initConnectorFunctions(nodeInfo, tableId, dataFlowDescriber.getId());

                            checkFunctions();

                            firstPatrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZING", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    verifyBatchRecordNotExist();
                                    firstRecord = (Map<String, Object>)firstPatrolEvent.getInfo("tdd_return_first");
                                }
                            });
                            firstPatrolEvent.addInfo("tdd_return_first", true);
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), firstPatrolEvent);
                        } else if(toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if(nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    verifyBatchRecordExists();
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
                    $(() -> {
                        throw ((AssertionFailedError) throwable);
                    });
                } else {
                    $(() -> Assertions.fail("Unknown error " + throwable.getMessage()));
                }
            }
        }, TapNodeInfo.NODE_TYPE_TARGET, TapNodeInfo.NODE_TYPE_SOURCE_TARGET);
        waitCompleted(200000);
    }

    private void verifyBatchRecordNotExist() {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        DataMap match = new DataMap();
        match.put("id", "id_1");
        match.put("tapString", "123");
        TapFilter filter = new TapFilter();
        filter.setMatch(match);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(match) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNull(filterResult.getError(), "Should be no value, error should not be threw"));
        $(() -> Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record hasn't be inserted yet"));
    }

    private void verifyBatchRecordExists() {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        DataMap match = new DataMap();
        match.put("id", "id_1");
        match.put("tapString", "123");
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
        MapDifference<String, Object> difference = Maps.difference(firstRecord, result);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        StringBuilder builder = new StringBuilder("Differences: \n");
        boolean equalResult;
        boolean different = false;
        for(Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            equalResult = false;
            MapDifference.ValueDifference<Object> diff = entry.getValue();
//            if(entry.getKey().equals("tapBinary")) {
//            }
            if((diff.leftValue() instanceof byte[]) && (diff.rightValue() instanceof byte[])) {
                equalResult = Arrays.equals((byte[])diff.leftValue(), (byte[])diff.rightValue());
            }

            if(!equalResult) {
                different = true;
                builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                builder.append("\t\t").append("Left ").append(diff.leftValue()).append("\n");
                builder.append("\t\t").append("Right ").append(diff.rightValue()).append("\n");
            }
        }
        boolean finalDifferent = different;
        $(() -> Assertions.assertFalse(finalDifferent, builder.toString()));
    }

    private void checkFunctions() {
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getWriteRecordFunction(), "DMLFunction is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly"));
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();

//        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
//        TapCodecRegistry codecRegistry = new TapCodecRegistry();
//        targetNode.getConnector().registerCapabilities(connectorFunctions, codecRegistry);

        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }
}
