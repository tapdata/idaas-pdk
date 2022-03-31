package io.tapdata.pdk.cli.commands;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.basic.BasicTest;
import io.tapdata.pdk.tdd.tests.target.beginner.DMLTest;
import io.tapdata.pdk.tdd.tests.target.intermediate.CreateTableTest;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class TDDCli extends CommonCli {
    private static final String TAG = TDDCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File file;
    @CommandLine.Option(names = { "-t", "--testCase" }, required = false, description = "Specify the test class simple name to test")
    private String testClass;
    @CommandLine.Option(names = { "-c", "--testConfig" }, required = true, description = "Specify the test json configuration file")
    private String testConfig;
    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;
    private SummaryGeneratingListener listener = new SummaryGeneratingListener();
    public void runOne(String testClass) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass("io.tapdata.pdk.tdd.tests." + testClass))
                .build();
        runTests(request);
    }

    public void runLevel(List<DiscoverySelector> selectors) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
//                .selectors(selectPackage("io.tapdata.pdk.tdd.tests.basic"),
//                        selectPackage("io.tapdata.pdk.tdd.tests.source." + level),
//                        selectPackage("io.tapdata.pdk.tdd.tests.target." + level))
                .selectors(selectors)
//                .filters(includeClassNamePatterns(".*Test"))
                .build();
        runTests(request);
    }

    public static final String LEVEL_BEGINNER = "beginner";
    public static final String LEVEL_INTERMEDIATE = "intermediate";
    public static final String LEVEL_EXPERT = "expert";

    private void runTests(LauncherDiscoveryRequest request) {
        Launcher launcher = LauncherFactory.create();
//        TestPlan testPlan = launcher.discover(request);
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        TestExecutionSummary summary = listener.getSummary();
        summary.printTo(new PrintWriter(System.out));
        summary.printFailuresTo(new PrintWriter(System.out));
        if(summary.getTestsFailedCount() > 0) {
//            throw new CoreException(ErrorCodes.TDD_TEST_FAILED, "Terminated because test failed");
            System.exit(0);
        }
    }

    public Integer execute() {
        try {
            testPDKJar(file, testConfig);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            PDKLogger.fatal(TAG, "Run test against file {} failed, {}", file, throwable.getMessage());
        }
        return 0;
    }

    private void testPDKJar(File file, String testConfig) throws Throwable {
        CommonUtils.setProperty("pdk_test_jar_file", file.getAbsolutePath());
        CommonUtils.setProperty("pdk_test_config_file", testConfig);

        PDKTestBase testBase = new PDKTestBase();
//        testBase.setup();
        TapConnector testConnector = testBase.getTestConnector();
        testBase.setup();

        DataMap testOptions = testBase.getTestOptions();

        String pdkId = null;
        if(testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }

        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for(TapNodeInfo tapNodeInfo : tapNodeInfoCollection) {
            if(pdkId != null) {
                if(tapNodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                    runLevel(generateTestTargets(tapNodeInfo));
                    break;
                }
            } else {
                PDKLogger.enable(true);
                runLevel(generateTestTargets(tapNodeInfo));
            }
        }
        System.exit(0);
    }

    private List<DiscoverySelector> generateTestTargets(TapNodeInfo tapNodeInfo) throws Throwable {
        StringBuilder builder = new StringBuilder();

        io.tapdata.pdk.apis.TapConnector connector = (io.tapdata.pdk.apis.TapConnector) tapNodeInfo.getNodeClass().getConstructor().newInstance();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecRegistry codecRegistry = new TapCodecRegistry();
        connector.registerCapabilities(connectorFunctions, codecRegistry);


        builder.append("\n-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
        builder.append("             Node class " + tapNodeInfo.getNodeClass() + " run tests below, ").append("\n");
        List<DiscoverySelector> selectors = new ArrayList<>();
        if(testClass != null) {
            Class<?> theClass = Class.forName(testClass);
            selectors.add(DiscoverySelectors.selectClass(theClass));
        } else {
            selectors.add(DiscoverySelectors.selectClass(BasicTest.class));

            if(connectorFunctions.getWriteRecordFunction() != null) {
                selectors.add(DiscoverySelectors.selectClass(DMLTest.class));
            }

            if(connectorFunctions.getCreateTableFunction() != null) {
                selectors.add(DiscoverySelectors.selectClass(CreateTableTest.class));
            }
        }
        builder.append("             Will run total " + selectors.size() + " test cases").append("\n");
        for(DiscoverySelector selector : selectors) {
            builder.append("             Test case " + selector.toString()).append("\n");
        }
        builder.append("-------------PDK connector idAndGroupAndVersion " + tapNodeInfo.getTapNodeSpecification().idAndGroup() + "-------------").append("\n");
        PDKLogger.info(TAG, builder.toString());
        return selectors;
    }

}
