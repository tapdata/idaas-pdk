//package io.tapdata.pdk.cli.commands;
//
//import com.alibaba.fastjson.JSON;
//import io.tapdata.entity.logger.TapLogger;
//import io.tapdata.entity.utils.DataMap;
//import io.tapdata.pdk.cli.CommonCli;
//import io.tapdata.pdk.cli.entity.DAGDescriber;
//import io.tapdata.pdk.core.api.ConnectionNode;
//import io.tapdata.pdk.core.api.PDKIntegration;
//import io.tapdata.pdk.core.connector.TapConnector;
//import io.tapdata.pdk.core.tapnode.TapNodeInfo;
//import io.tapdata.pdk.core.utils.CommonUtils;
//import io.tapdata.pdk.tdd.core.PDKTestBase;
//import org.apache.commons.io.FilenameUtils;
//import org.apache.maven.cli.MavenCli;
//import org.apache.maven.model.Model;
//import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
//import org.apache.maven.shared.invoker.*;
//import picocli.CommandLine;
//
//import java.io.File;
//import java.io.FileReader;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.UUID;
//
//@CommandLine.Command(
//        description = "Model deduction method",
//        subcommands = MainCli.class
//)
//public class ModelDeductionCli extends CommonCli {
//    private static final String TAG = ModelDeductionCli.class.getSimpleName();
//
//    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
//    File[] files;
//
//    @CommandLine.Option(names = { "-o", "--output" }, required = true, description = "Specify the folder where model deduction report files will be generated")
//    private String output;
//
//    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
//    private boolean helpRequested = false;
//
//    public Integer execute() throws Exception {
//        for(File file : files) {
//
//            String jarFile = null;
//            if(file.isFile()) {
//                jarFile = file.getAbsolutePath();
//            } else if(file.isDirectory()) {
//                if(!file.getAbsolutePath().contains("connectors")) {
//                    throw new IllegalArgumentException("Connector project is under connectors directory, are you passing the correct connector project directory? " + file.getAbsolutePath());
//                }
//
//                if(installProjects != null && mavenHome != null) {
//                    System.setProperty("maven.home", mavenHome);
//                    for(String installProject : installProjects) {
//                        String pomFile = installProject;
//                        if(!pomFile.equals("pom.xml")) {
//                            pomFile = pomFile + File.separator + "pom.xml";
//                        }
////                    int state = mavenCli.doMain(new String[]{"install", "-f", pomFile}, "./", System.out, System.out);
//
//                        InvocationRequest request = new DefaultInvocationRequest();
//                        request.setPomFile( new File( pomFile ) );
//                        request.setGoals( Collections.singletonList( "install" ) );
//
//                        Invoker invoker = new DefaultInvoker();
//                        InvocationResult result = invoker.execute( request );
//
//                        if ( result.getExitCode() != 0 )
//                        {
//                            if(result.getExecutionException() != null)
//                                System.out.println(result.getExecutionException().getMessage());
//                            System.out.println("------------- Dependency project " + pomFile + " installed Failed --------------");
//                            System.exit(0);
//                        } else {
//                            System.out.println("------------- Dependency project " + pomFile + " installed successfully -------------");
//                        }
//                    }
//                }
//
//                System.setProperty("maven.multiModuleProjectDirectory", file.getAbsolutePath());
//                System.out.println(file.getName() + " is packaging...");
//                MavenCli mavenCli = new MavenCli();
//                int state = mavenCli.doMain(new String[]{"clean", "package", "-DskipTests", "-P", "not_encrypt", "-U"}, file.getAbsolutePath(), System.out, System.out);
//                if (0 == state){
//                    MavenXpp3Reader reader = new MavenXpp3Reader();
//                    Model model = reader.read(new FileReader(FilenameUtils.concat(file.getAbsolutePath(), "pom.xml")));
//                    jarFile = FilenameUtils.concat("./", "./dist/" + model.getArtifactId() + "-v" + model.getVersion() + ".jar");
//                    System.out.println("------------- Maven package successfully -------------");
//                    System.out.println("Connector jar is " + jarFile);
////                System.setProperty("maven.multiModuleProjectDirectory", ".");
//                    Thread.currentThread().setContextClassLoader(TDDCli.class.getClassLoader());
//                } else {
//                    System.out.println("");
//                    System.out.println("------------- Maven package Failed --------------");
//                    System.exit(0);
//                }
//            } else {
//                throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not exist");
//            }
//
//            CommonUtils.setProperty("pdk_test_jar_file", jarFile);
//            CommonUtils.setProperty("pdk_test_config_file", testConfig);
//
//            PDKTestBase testBase = new PDKTestBase();
////        testBase.setup();
//            TapConnector testConnector = testBase.getTestConnector();
//            testBase.setup();
//
//            DataMap testOptions = testBase.getTestOptions();
//
//            testBase.tearDown();
//
//            String pdkId = null;
//            if(testOptions != null) {
//                pdkId = (String) testOptions.get("pdkId");
//            }
//
//            Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
//            for(TapNodeInfo tapNodeInfo : tapNodeInfoCollection) {
//                if(pdkId != null) {
//                    if(tapNodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
//                        runLevelWithNodeInfo(tapNodeInfo);
//                        break;
//                    }
//                } else {
////                PDKLogger.enable(true);
//                    runLevelWithNodeInfo(tapNodeInfo);
//                }
//            }
//            System.out.println("*****************************************************TDD Results*****************************************************");
//            for(TDDCli.TapSummary testSummary : testResultSummaries) {
//                StringBuilder builder = new StringBuilder();
//                builder.append("-------------PDK id '" + testSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testSummary.tapNodeInfo.getNodeClass().getName() + "'-------------").append("\n");
//                builder.append("             Node class " + testSummary.tapNodeInfo.getNodeClass() + " run ");
//
//                builder.append(testSummary.testClasses.size() + " test classes").append("\n");
//                for(Class<?> testClass : testSummary.testClasses) {
//                    builder.append("             \t" + testClass.getName()).append("\n");
//                }
//                builder.append("\n");
//                outputTestResult(testSummary, builder);
//
//                builder.append("-------------PDK id '" + testSummary.tapNodeInfo.getTapNodeSpecification().getId() + "' class '" + testSummary.tapNodeInfo.getNodeClass().getName() + "'-------------").append("\n");
//                System.out.print(builder.toString());
//            }
//            System.out.println("*****************************************************TDD Results*****************************************************");
//            System.out.println("Congratulations! PDK " + jarFile + " has passed all tests!");
//            System.exit(0);
//        }
//        return 0;
//    }
//
//}
