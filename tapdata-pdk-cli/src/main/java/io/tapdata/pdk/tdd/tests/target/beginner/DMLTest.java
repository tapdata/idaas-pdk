package io.tapdata.pdk.tdd.tests.target.beginner;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
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
                                    insertOneRecord(dataFlowEngine, dag);
                                }
                            });
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
                            //TODO Use PatrolEvent to let TDD Source send a TapUpdateRecordEvent
                            //TODO Send PatrolEvent again to verify update successfully.
//                                    verifyUpdate();
                            //TODO Do delete test after update. it is async.
                            //TODO Use PatrolEvent to let TDD Source send a TapDeleteRecordEvent
                            //TODO Send PatrolEvent again to verify delete successfully.
//                                    verifyDelete();

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
//        completed();
        waitCompleted(20000);

    }

    private void sendDeletePatrolEvent(DataFlowEngine dataFlowEngine, TapDAG dag) {
        PatrolEvent deletePatrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                deleteOneRecord(dataFlowEngine, dag);
            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(), deletePatrolEvent);
    }

    private void sendUpdatePatrolEvent(DataFlowEngine dataFlowEngine, TapDAG dag) {
        PatrolEvent updatePatrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                updateOneRecord(dataFlowEngine, dag);
            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(), updatePatrolEvent);
    }

    private void insertOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        firstRecord = new DataMap();
        firstRecord.put("id", "id_2");
        firstRecord.put("tapString", "1234");
        firstRecord.put("tapString10", "0987654321");
        firstRecord.put("tapInt", 123123);
        firstRecord.put("tapBoolean", true);
        firstRecord.put("tapArrayString", Arrays.asList("1", "2", "3"));
        firstRecord.put("tapArrayDouble", Arrays.asList(1.1, 2.2, 3.3));
        firstRecord.put("tapNumber", 1233);
        // firstRecord.put("tapNumber(8)", 1111),
        firstRecord.put("tapNumber52", 343.22);
        firstRecord.put("tapBinary", new byte[]{123, 21, 3, 2});
        HashMap<String, Object> tapMapStringString = new HashMap<>();
        tapMapStringString.put("a", "a");
        tapMapStringString.put("b", "b");
        firstRecord.put("tapMapStringString", tapMapStringString);
        HashMap<String, Object> tapMapStringDouble = new HashMap<>();
        tapMapStringDouble.put("a", 1.0);
        tapMapStringDouble.put("b", 2.0);
        firstRecord.put("tapMapStringDouble", tapMapStringDouble);
        insertRecordEvent.setAfter(firstRecord);

        dataFlowEngine.sendExternalTapEvent(dag.getId(), insertRecordEvent);
        PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyBatchRecordExists();
                sendUpdatePatrolEvent(dataFlowEngine,dag);
            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }


    private void updateOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap after = new DataMap();
        DataMap before = new DataMap();
        before.put("id", "id_2");
        before.put("tapString", "1234");
        after.put("id", "id_2");
        after.put("tapString", "1234");
        after.put("tapInt", "5555");
        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        tapUpdateRecordEvent.setAfter(after);
        tapUpdateRecordEvent.setBefore(before);

        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapUpdateRecordEvent);
        PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyUpdateOneRecord();
                sendDeletePatrolEvent(dataFlowEngine,dag);
            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(),patrolEvent);
    }

    private void verifyUpdateOneRecord() {
        firstRecord.put("id", "id_2");
        firstRecord.put("tapString", "1234");
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        filter.setMatch(firstRecord);
        List<TapFilter> filters = Collections.singletonList(filter);
        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(firstRecord) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNotNull(filterResult.getResult().get("tapInt"), "The value of tapInt should not be null"));
        $(() -> Assertions.assertEquals("5555", filterResult.getResult().get("tapInt"), "The value of \"tapInt\" should not be \"5555\""));
    }

    private void deleteOneRecord(DataFlowEngine dataFlowEngine, TapDAG dag) {
        DataMap before = new DataMap();
        before.put("id", "id_2");
        before.put("tapString", "1234");
        TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
        tapDeleteRecordEvent.setBefore(before);

        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDeleteRecordEvent);
        PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                verifyDeleteOneRecord();
            }
        });
        dataFlowEngine.sendExternalTapEvent(dag.getId(),patrolEvent);
    }


    private void verifyDeleteOneRecord() {
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
        $(() -> Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record hasn't be inserted yet"));
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

        MapDifference<String, Object> difference = Maps.difference(firstRecord, result);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        StringBuilder builder = new StringBuilder("Differences: \n");
        boolean equalResult;
        boolean different = false;
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            equalResult = false;
            MapDifference.ValueDifference<Object> diff = entry.getValue();
//            if(entry.getKey().equals("tapBinary")) {
//            }
            if ((diff.leftValue() instanceof byte[]) && (diff.rightValue() instanceof byte[])) {
                equalResult = Arrays.equals((byte[]) diff.leftValue(), (byte[]) diff.rightValue());
            }

            if (!equalResult) {
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
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getWriteRecordFunction(), "WriteRecord is a must to implement a Target"));
        $(() -> Assertions.assertNotNull(targetNode.getConnectorFunctions().getQueryByFilterFunction(), "QueryByFilter is needed for TDD to verify the record is written correctly"));
    }

    private void initConnectorFunctions(TapNodeInfo nodeInfo, String tableId, String dagId) {
        targetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        tddSourceNode = dataFlowWorker.getSourceNodeDriver(sourceNodeId).getSourceNode();
    }
}
