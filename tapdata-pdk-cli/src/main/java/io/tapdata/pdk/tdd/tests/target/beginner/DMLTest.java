package io.tapdata.pdk.tdd.tests.target.beginner;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
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
import java.util.concurrent.atomic.AtomicLong;

@DisplayName("Tests for target beginner test")
public class DMLTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    TargetNode targetNode;
    SourceNode tddSourceNode;
    PatrolEvent firstPatrolEvent;
    Map<String, Object> firstRecord;
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

                            firstPatrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZING", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    insertOneRecord();
                                }
                            });

                            dataFlowEngine.sendExternalTapEvent(dag.getId(), firstPatrolEvent);
                        } else if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                            PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                PDKLogger.info("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    verifyBatchRecordExists();
                                    //TODO Use PatrolEvent to let TDD Source send a TapUpdateRecordEvent
                                    //TODO Send PatrolEvent again to verify update successfully.
//                                    verifyUpdate();

                                    //TODO Do delete test after update. it is async.
                                    //TODO Use PatrolEvent to let TDD Source send a TapDeleteRecordEvent
                                    //TODO Send PatrolEvent again to verify delete successfully.
//                                    verifyDelete();
                                }
                            });
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);

                            patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    updateOneRecord();
                                }
                            });
                            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);

                            patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                                if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                    deleteOneRecord();
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
        }, TapNodeInfo.NODE_TYPE_TARGET, TapNodeInfo.NODE_TYPE_SOURCE_TARGET);
//        completed();
        waitCompleted(20000);

    }

    private void insertOneRecord() {
        WriteRecordFunction writeRecordFunction = targetNode.getConnectorFunctions().getWriteRecordFunction();
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        firstRecord = new DataMap();
        firstRecord.put("id", "id_1");
        firstRecord.put("tapString", "123");
        firstRecord.put("tapString10", "1234567890");
        firstRecord.put("tapString10Fixed", "1");
        firstRecord.put("tapInt", 123123);
        firstRecord.put("tapBoolean", true);
        firstRecord.put("tapArrayString", Arrays.asList("1", "2", "3"));
        firstRecord.put("tapArrayDouble", Arrays.asList(1.1, 2.2, 3.3));
        firstRecord.put("tapNumber", 1233);
        // firstRecord.put("tapNumber(8)", 1111),
        firstRecord.put("tapNumber52", 343.22);
        firstRecord.put("tapBinary", new byte[]{123, 21, 3, 2});
        HashMap<Object, Object> tapMapStringString = new HashMap<>();
        tapMapStringString.put("a", "a");
        tapMapStringString.put("b", "b");
        firstRecord.put("tapMapStringString", tapMapStringString);
        HashMap<Object, Object> tapMapStringDouble = new HashMap<>();
        tapMapStringDouble.put("a", 1.0);
        tapMapStringDouble.put("b", 2.0);
        firstRecord.put("tapMapStringDouble", tapMapStringDouble);


        List<TapRecordEvent> recordEvents = new ArrayList<>();
        insertRecordEvent.setAfter(firstRecord);
        AtomicLong insertCount = new AtomicLong();
        recordEvents.add(insertRecordEvent);
        CommonUtils.handleAnyError(() -> writeRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents,
                writeConsumer -> insertCount.set(writeConsumer.getInsertedCount())));
        $(() -> Assertions.assertEquals(1, insertCount.get(), "The number of insert count should be 1"));
        verifyBatchRecordExists();
    }


    private void updateOneRecord() {
        WriteRecordFunction writeRecordFunction = targetNode.getConnectorFunctions().getWriteRecordFunction();
        DataMap after = new DataMap();
        after.put("id", "id_1");
        after.put("tapString", "123");
        after.put("tapInt", "5555");
        List<TapRecordEvent> recordEvents = new ArrayList<>();
        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        tapUpdateRecordEvent.setAfter(after);
        recordEvents.add(tapUpdateRecordEvent);
        AtomicLong modifyCount = new AtomicLong();
        CommonUtils.handleAnyError(() -> writeRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents,
                writeConsumer -> modifyCount.set(writeConsumer.getModifiedCount())));
        $(() -> Assertions.assertEquals(1, modifyCount.get(), "The number of modify count should be 1"));

        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        after.remove("tapInt");
        filter.setMatch(after);
        List<TapFilter> filters = Collections.singletonList(filter);
        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(after) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNotNull(filterResult.getResult().get("tapInt"),"The value of tapInt should not be null"));
        $(() -> Assertions.assertEquals("5555", filterResult.getResult().get("tapInt"),"The value of \"tapInt\" should not be \"5555\""));
    }

    private void deleteOneRecord() {
        WriteRecordFunction writeRecordFunction = targetNode.getConnectorFunctions().getWriteRecordFunction();
        DataMap before = new DataMap();
        before.put("id", "id_1");
        before.put("tapString", "123");
        List<TapRecordEvent> recordEvents = new ArrayList<>();
        TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
        tapDeleteRecordEvent.setBefore(before);
        recordEvents.add(tapDeleteRecordEvent);
        AtomicLong removedCount = new AtomicLong();
        CommonUtils.handleAnyError(() -> writeRecordFunction.writeDML(targetNode.getConnectorContext(), recordEvents,
                writeConsumer -> removedCount.set(writeConsumer.getRemovedCount())));
        $(() -> Assertions.assertEquals(1, removedCount.get(), "The number of modifications should be 1"));

        verifyBatchRecordNotExist();
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
