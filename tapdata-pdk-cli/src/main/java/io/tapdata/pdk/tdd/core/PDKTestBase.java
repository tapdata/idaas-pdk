package io.tapdata.pdk.tdd.core;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.api.SourceAndTargetNode;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
    private Throwable lastThrowable;

    public PDKTestBase() {
        String testConfig = CommonUtils.getProperty("pdk_test_config_file", "");
        testConfigFile = new File(testConfig);
        if(!testConfigFile.isFile())
            throw new IllegalArgumentException("TDD test config file doesn't exist or not a file, please check " + testConfigFile);

        String jarUrl = CommonUtils.getProperty("pdk_test_jar_file", "");
        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "./dist") + "/tdd-connector-v1.0-SNAPSHOT.jar";
        File tddJarFile = new File(tddJarUrl);
        if(!tddJarFile.isFile())
            throw new IllegalArgumentException("TDD jar file doesn't exist or not a file, please check " + tddJarFile);

        if(StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        jarFile = new File(jarUrl);
        if(!jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarUrl + " is not a file or not exists");
        TapConnectorManager.getInstance().start(Arrays.asList(jarFile, tddJarFile));
        testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for(TapNodeInfo nodeInfo : tapNodeInfoCollection) {
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

    public interface AssertionCall {
        void assertIt();
    }

    public void $(AssertionCall assertionCall) {
        try {
            assertionCall.assertIt();
        } catch(Throwable throwable) {
            lastThrowable = throwable;
            completed();
        }
    }

    public void completed() {
        if(completed.compareAndSet(false, true)) {
            PDKLogger.enable(false);
            synchronized (completed) {
                completed.notifyAll();
            }
        }
    }

    public void waitCompleted(long seconds) throws Throwable {
        while (!completed.get()) {
            synchronized (completed) {
                if(completed.get()) {
                    try {
                        completed.wait(seconds * 1000);
                        completed.set(true);
                        if(lastThrowable == null)
                            throw new TimeoutException("Waited " + seconds + " seconds and still not completed, consider timeout execution.");
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                        PDKLogger.error(TAG, "Completed wait interrupted " + interruptedException.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        if(lastThrowable != null)
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
        return jsonParser.fromJson(testConfigJson, new TypeHolder<Map<String, DataMap>>(){});
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

    public void consumeQualifiedTapNodeInfo(Consumer<TapNodeInfo> consumer, String... nodeTypes) {
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if(tapNodeInfoCollection.isEmpty())
            throw new CoreException(ErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + jarFile);

        String pdkId = null;
        if(testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }

        List<String> nodeTypeList = Arrays.asList(nodeTypes);
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            if(pdkId != null) {
                if(nodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                    consumer.accept(nodeInfo);
                    break;
                }
            } else if(nodeTypeList.contains(nodeInfo.getNodeType())) {
                consumer.accept(nodeInfo);
                break;
            }
        }
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
