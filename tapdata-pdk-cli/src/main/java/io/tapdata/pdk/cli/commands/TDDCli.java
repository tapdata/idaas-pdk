package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.services.UploadFileService;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import picocli.CommandLine;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.*;

import static org.junit.platform.engine.discovery.ClassNameFilter.includeClassNamePatterns;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class TDDCli extends CommonCli {
    private static final String TAG = TDDCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File[] files;
    @CommandLine.Option(names = { "-t", "--test" }, required = false, description = "Specify the test class simple name to test")
    private String testClass;
    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;
    private SummaryGeneratingListener listener = new SummaryGeneratingListener();
    public void runOne(String testClass) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass("io.tapdata.pdk.tdd.tests." + testClass))
                .build();
        runTests(request);
    }

    public void runAll() {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectPackage("io.tapdata.pdk.tdd.tests"))
                .filters(includeClassNamePatterns(".*Test"))
                .build();
        runTests(request);
    }

    private void runTests(LauncherDiscoveryRequest request) {
        Launcher launcher = LauncherFactory.create();
//        TestPlan testPlan = launcher.discover(request);
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        TestExecutionSummary summary = listener.getSummary();
        summary.printTo(new PrintWriter(System.out));
        summary.printFailuresTo(new PrintWriter(System.out));
    }

    public Integer execute() throws Exception {
        if(testClass != null) {
            runOne(testClass);
        } else {
            runAll();
        }
        for(File file : files) {
            testPDKJar(file);
        }
//        try {
//            TapConnectorManager.getInstance().start(Arrays.asList(files));
//
//            for(File file : files) {
//              List<String> jsons = new ArrayList<>();
//              TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
//              Collection<TapNodeInfo> tapNodeInfoCollection = connector.getTapNodeClassFactory().getConnectorTapNodeInfos();
//              Map<String, InputStream> inputStreamMap = new HashMap<>();
//              for(TapNodeInfo nodeInfo : tapNodeInfoCollection) {
//                TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
//                String iconPath = specification.getIcon();
//                if (StringUtils.isNotBlank(iconPath)) {
//                  InputStream is = nodeInfo.readResource(iconPath);
//                  if (is != null) {
//                    inputStreamMap.put(iconPath, is);
//                  }
//                }
//                JSONObject o = (JSONObject)JSON.toJSON(specification);
//                o.put("type", nodeInfo.getNodeType());
//                // get the version info and group info from jar
//                o.put("version", nodeInfo.getNodeClass().getPackage().getImplementationVersion());
//                o.put("group", nodeInfo.getNodeClass().getPackage().getImplementationVendor());
//                String jsonString = o.toJSONString();
//                jsons.add(jsonString);
//              }
//              System.out.println(tapNodeInfoCollection);
//              UploadFileService.upload(inputStreamMap, file, jsons, latest, tmUrl, authToken);
//            }
//        } catch (Throwable throwable) {
//            CommonUtils.logError(TAG, "Start failed", throwable);
//        }
        return 0;
    }

    private void testPDKJar(File file) {
        CommonUtils.setProperty("pdk_test_jar_file", file.getAbsolutePath());

    }

}
