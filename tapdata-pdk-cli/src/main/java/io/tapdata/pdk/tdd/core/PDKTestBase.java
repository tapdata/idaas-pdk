package io.tapdata.pdk.tdd.core;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.*;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.DataFlowEngine;
import io.tapdata.pdk.core.workflow.engine.TapDAG;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PDKTestBase {
    private static final String TAG = PDKTestBase.class.getSimpleName();
    protected TapConnector testConnector;
    protected TapConnector tddConnector;
    protected File testConfigFile;
    protected File jarFile;

    protected DataMap connectionOptions;
    protected DataMap nodeOptions;
    protected DataMap testOptions;

    private final AtomicBoolean completed = new AtomicBoolean(false);
    private boolean finishSuccessfully = false;
    private Throwable lastThrowable;

    protected TapDAG dag;

    public PDKTestBase() {
        String testConfig = CommonUtils.getProperty("pdk_test_config_file", "");
        testConfigFile = new File(testConfig);
        if (!testConfigFile.isFile())
            throw new IllegalArgumentException("TDD test config file doesn't exist or not a file, please check " + testConfigFile);

        String jarUrl = CommonUtils.getProperty("pdk_test_jar_file", "");
        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "./dist") + "/tdd-connector-v1.0-SNAPSHOT.jar";
        File tddJarFile = new File(tddJarUrl);
        if (!tddJarFile.isFile())
            throw new IllegalArgumentException("TDD jar file doesn't exist or not a file, please check " + tddJarFile);

        if (StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        jarFile = new File(jarUrl);
        if (!jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarUrl + " is not a file or not exists");
        TapConnectorManager.getInstance().start(Arrays.asList(jarFile, tddJarFile));
        testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
            String iconPath = specification.getIcon();
            PDKLogger.info(TAG, "Found connector name {} id {} group {} version {} icon {}", specification.getName(), specification.getId(), specification.getGroup(), specification.getVersion(), specification.getIcon());
            if (StringUtils.isNotBlank(iconPath)) {
                InputStream is = nodeInfo.readResource(iconPath);
                if (is == null) {
                    PDKLogger.error(TAG, "Icon image file doesn't be found for url {} which defined in spec json file.");
                }
            }
        }

        tddConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(tddJarFile.getName());
        PDKInvocationMonitor.getInstance().setErrorListener(errorMessage -> $(() -> Assertions.fail(errorMessage)));
    }

    public String testTableName(String id) {
        return id.replace('-', '_').replace('>', '_') + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public interface AssertionCall {
        void assertIt();
    }

    public void $(AssertionCall assertionCall) {
        try {
            assertionCall.assertIt();
        } catch (Throwable throwable) {
            lastThrowable = throwable;
            completed(true);
        }
    }

    public void completed() {
        completed(false);
    }

    public void completed(boolean withError) {
        if (completed.compareAndSet(false, true)) {
            finishSuccessfully = !withError;
            PDKLogger.enable(false);
            synchronized (completed) {
                completed.notifyAll();
            }
        }
    }

    public void waitCompleted(long seconds) throws Throwable {
        while (!completed.get()) {
            synchronized (completed) {
                if (completed.get()) {
                    try {
                        completed.wait(seconds * 1000);
                        completed.set(true);
                        if (lastThrowable == null && !finishSuccessfully)
                            throw new TimeoutException("Waited " + seconds + " seconds and still not completed, consider timeout execution.");
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                        PDKLogger.error(TAG, "Completed wait interrupted " + interruptedException.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        if (lastThrowable != null)
            throw lastThrowable;
    }

    public Map<String, DataMap> readTestConfig(File testConfigFile) {
        String testConfigJson = null;
        try {
            testConfigJson = FileUtils.readFileToString(testConfigFile, "utf8");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CoreException(ErrorCodes.TDD_READ_TEST_CONFIG_FAILED, "Test config file " + testConfigJson + " read failed, " + e.getMessage());
        }
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        return jsonParser.fromJson(testConfigJson, new TypeHolder<Map<String, DataMap>>() {
        });
    }

    public void prepareConnectionNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectionNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createConnectionConnectorBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareSourceNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectorNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createSourceBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareTargetNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectorNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createTargetBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareSourceAndTargetNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<SourceAndTargetNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createSourceAndTargetBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void consumeQualifiedTapNodeInfo(Consumer<TapNodeInfo> consumer) {
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(ErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + jarFile);

        String pdkId = null;
        if (testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }
        if (pdkId == null) {
            pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
            if (pdkId == null)
                Assertions.fail("Test pdkId is not specified");
        }
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            if (nodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                consumer.accept(nodeInfo);
                break;
            }
        }
    }

    @BeforeAll
    public static void setupAll() {
        DataFlowEngine.getInstance().start();
    }

    @BeforeEach
    public void setup() {
        PDKLogger.info(TAG, "setup");
        Map<String, DataMap> testConfigMap = readTestConfig(testConfigFile);
        Assertions.assertNotNull(testConfigMap, "testConfigFile " + testConfigFile + " read to json failed");
        connectionOptions = testConfigMap.get("connection");
        nodeOptions = testConfigMap.get("node");
        testOptions = testConfigMap.get("test");
    }

    @AfterEach
    public void tearDown() {
        PDKLogger.info(TAG, "tearDown");
        if (dag != null) {
            DataFlowEngine.getInstance().stopDataFlow(dag.getId());
        }
    }

    public DataMap getTestOptions() {
        return testOptions;
    }

    protected boolean mapEquals(Map<String, Object> firstRecord, Map<String, Object> result, StringBuilder builder) {
        MapDifference<String, Object> difference = Maps.difference(firstRecord, result);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        builder.append("Differences: \n");
        boolean different = false;
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            MapDifference.ValueDifference<Object> diff = entry.getValue();
            Object leftValue = diff.leftValue();
            Object rightValue = diff.rightValue();

            boolean equalResult = objectIsEqual(leftValue, rightValue);

            if (!equalResult) {
                different = true;
                builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                builder.append("\t\t").append("Left ").append(diff.leftValue()).append(" class ").append(diff.leftValue().getClass().getSimpleName()).append("\n");
                builder.append("\t\t").append("Right ").append(diff.rightValue()).append(" class ").append(diff.rightValue().getClass().getSimpleName()).append("\n");
            }
        }
        return different;
    }

    public boolean objectIsEqual(Object leftValue, Object rightValue) {
        boolean equalResult = false;
//        if ((leftValue instanceof List) && (rightValue instanceof List)) {
//            if (((List<?>) leftValue).size() == ((List<?>) rightValue).size()) {
//                for (int i = 0; i < ((List<?>) leftValue).size(); i++) {
//                    equalResult = objectIsEqual(((List<?>) leftValue).get(i), ((List<?>) rightValue).get(i));
//                    if (!equalResult) break;
//                }
//            }
//        }

        if ((leftValue instanceof byte[]) && (rightValue instanceof byte[])) {
            equalResult = Arrays.equals((byte[]) leftValue, (byte[]) rightValue);
        } else if ((leftValue instanceof byte[]) && (rightValue instanceof String)) {
            //byte[] vs string, base64 decode string
            try {
//                    byte[] rightBytes = Base64.getDecoder().decode((String) rightValue);
                byte[] rightBytes = Base64.decodeBase64((String) rightValue);
                equalResult = Arrays.equals((byte[]) leftValue, rightBytes);
            } catch (Throwable ignored) {
            }
        } else if ((leftValue instanceof Number) && (rightValue instanceof Number)) {
            //number vs number, equal by value
            BigDecimal leftB = null;
            BigDecimal rightB = null;
            if (leftValue instanceof BigDecimal) {
                leftB = (BigDecimal) leftValue;
            }
            if (rightValue instanceof BigDecimal) {
                rightB = (BigDecimal) rightValue;
            }
            if (leftB == null) {
                leftB = BigDecimal.valueOf(((Number) leftValue).doubleValue());
            }
            if (rightB == null) {
                rightB = BigDecimal.valueOf(((Number) rightValue).doubleValue());
            }
            equalResult = leftB.compareTo(rightB) == 0;
        } else if ((leftValue instanceof Boolean)) {
            if (rightValue instanceof Number) {
                //boolean true == (!=0), false == 0
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((Number) rightValue).longValue() != 0;
                } else {
                    equalResult = ((Number) rightValue).longValue() == 0;
                }
            } else if (rightValue instanceof String) {
                //boolean true == "true", false == "false"
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((String) rightValue).equalsIgnoreCase("true");
                } else {
                    equalResult = ((String) rightValue).equalsIgnoreCase("false");
                }
            }
        }else{
            equalResult = leftValue.equals(rightValue);
        }
        return equalResult;
    }

    public DataMap buildInsertRecord() {
        DataMap insertRecord = new DataMap();
        insertRecord.put("id", "id_2");
        insertRecord.put("tapString", "1234");
        insertRecord.put("tapString10", "0987654321");
        insertRecord.put("tapInt", 123123);
        insertRecord.put("tapBoolean", true);
        insertRecord.put("tapNumber", 123.0);
        insertRecord.put("tapNumber52", 343.22);
        insertRecord.put("tapBinary", new byte[]{123, 21, 3, 2});
        return insertRecord;
    }

    public DataMap buildFilterMap() {
        DataMap filterMap = new DataMap();
        filterMap.put("id", "id_2");
        filterMap.put("tapString", "1234");
        return filterMap;
    }

    public DataMap buildUpdateMap() {
        DataMap updateMap = new DataMap();
        updateMap.put("id", "id_2");
        updateMap.put("tapString", "1234");
        updateMap.put("tapInt", 5555);
        return updateMap;
    }

    public void sendInsertRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, DataMap after, PatrolEvent patrolEvent) {
        TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
        tapInsertRecordEvent.setAfter(after);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapInsertRecordEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendUpdateRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, DataMap before, DataMap after, PatrolEvent patrolEvent) {
        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        tapUpdateRecordEvent.setAfter(after);
        tapUpdateRecordEvent.setBefore(before);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapUpdateRecordEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendDeleteRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, DataMap before, PatrolEvent patrolEvent) {
        TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
        tapDeleteRecordEvent.setBefore(before);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDeleteRecordEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendCreateTableEvent(DataFlowEngine dataFlowEngine, TapDAG dag, PatrolEvent patrolEvent) {
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
        dataFlowEngine.sendExternalTapEvent(dag.getId(), createTableEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendDropTableEvent(DataFlowEngine dataFlowEngine, TapDAG dag, PatrolEvent patrolEvent) {
        TapDropTableEvent tapDropTableEvent = new TapDropTableEvent();
        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDropTableEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    protected void verifyUpdateOneRecord(TargetNode targetNode, DataMap before, DataMap verifyRecord) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        filter.setMatch(before);
        List<TapFilter> filters = Collections.singletonList(filter);
        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(before) + " for queryByFilter, then filterResults size has to be 1. Please make sure writeRecord method update record correctly and queryByFilter can query it out for verification. "));
        FilterResult filterResult = results.get(0);

        $(() -> Assertions.assertNotNull(filterResult.getResult().get("tapInt"), "The value of tapInt should not be null"));
        for (Map.Entry<String, Object> entry : verifyRecord.entrySet()) {
            $(() -> Assertions.assertTrue(objectIsEqual(entry.getValue(), filterResult.getResult().get(entry.getKey())), "The value of \"" + entry.getKey() + "\" should be \"" + entry.getValue() + "\",but actual it is \"" + filterResult.getResult().get(entry.getKey()) + "\", please make sure TapUpdateRecordEvent is handled well in writeRecord method"));
        }
    }

    protected void verifyTableNotExists(TargetNode targetNode, DataMap filterMap) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        if(filterResult.getResult() == null) {
            $(() -> Assertions.assertNull(filterResult.getResult(), "Table does not exist, result should be null"));
        } else {
            $(() -> Assertions.assertNotNull(filterResult.getError(), "Table does not exist, error should be threw"));
        }
    }

    protected void verifyRecordNotExists(TargetNode targetNode, DataMap filterMap) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " for queryByFilter, then filterResults size has to be 1"));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNull(filterResult.getError(), "Should be no value, error should not be threw"));
        $(() -> Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record has been deleted, please make sure TapDeleteRecordEvent is handled well in writeRecord method."));
    }

    protected void verifyBatchRecordExists(SourceNode sourceNode, TargetNode targetNode, DataMap filterMap) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
        List<TapFilter> filters = Collections.singletonList(filter);

        List<FilterResult> results = new ArrayList<>();
        CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, results::addAll));
        $(() -> Assertions.assertEquals(results.size(), 1, "There is one filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " for queryByFilter, then filterResults size has to be 1. Please make sure writeRecord method write record correctly and queryByFilter can query it out for verification. "));
        FilterResult filterResult = results.get(0);
        $(() -> Assertions.assertNull(filterResult.getError(), "Error occurred while queryByFilter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " error " + filterResult.getError()));
        $(() -> Assertions.assertNotNull(filterResult.getResult(), "Result should not be null, as the record has been inserted"));
        Map<String, Object> result = filterResult.getResult();


        sourceNode.getCodecFilterManager().transformToTapValueMap(result, sourceNode.getConnectorContext().getTable().getNameFieldMap());
        sourceNode.getCodecFilterManager().transformFromTapValueMap(result);

        StringBuilder builder = new StringBuilder();
        $(() -> Assertions.assertFalse(mapEquals(filterMap, result, builder), builder.toString()));
    }

    public TapConnector getTestConnector() {
        return testConnector;
    }

    public File getTestConfigFile() {
        return testConfigFile;
    }

    public File getJarFile() {
        return jarFile;
    }

    public DataMap getConnectionOptions() {
        return connectionOptions;
    }

    public DataMap getNodeOptions() {
        return nodeOptions;
    }
}
