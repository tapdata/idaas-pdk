package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Lists;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.cli.CommonCli;
import org.apache.commons.io.FileUtils;
import org.apache.maven.cli.MavenCli;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.util.List;

/**
 * @author Dexter
 */
@CommandLine.Command(
  description = "Init a connector project",
  subcommands = MainCli.class
)
public class ConnectorProjectBootCli extends CommonCli {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorProjectBootCli.class.getSimpleName());
  private static final String TAG = ConnectorProjectBootCli.class.getSimpleName();

  @CommandLine.Option(names = {"-g", "--group"}, required = false, description = "The group id of the connector")
  private String groupId;

  @CommandLine.Option(names = {"-n", "--name"}, required = true, description = "The name of the connector")
  private String artifactId;

  @CommandLine.Option(names = {"-v", "--version"}, required = true, description = "The version of the connector")
  private String version;

  @CommandLine.Option(names = {"-o", "--output"}, required = true, description = "The location of the connector project")
  private String output;

  @Override
  protected Integer execute() throws Exception {
    List<String> paramsList = Lists.newArrayList();
    paramsList.add("archetype:generate");
    paramsList.add("-DarchetypeGroupId=io.tapdata.pdk");
    paramsList.add("-DarchetypeArtifactId=source-target-connector-archetype");
    paramsList.add("-DarchetypeVersion=1.0.0");
    paramsList.add("-DinteractiveMode=false");
    //debug only
    paramsList.add("-DarchetypeCatalog=local");
    paramsList.add(String.format("-DgroupId=%s", groupId));
    paramsList.add(String.format("-DartifactId=%s-connector", artifactId.toLowerCase()));
    paramsList.add(String.format("-Dpackage=%s.%s", groupId, artifactId.toLowerCase()));
    paramsList.add(String.format("-DlibName=%s", artifactId));

    if (output.startsWith(".")) {
      output = System.getProperty("user.dir") + output.substring(1);
    }
    paramsList.add(String.format("-DoutputDirectory=%s", output));

    MavenCli mavenCli = new MavenCli(new ClassWorld("maven", getClass().getClassLoader()));
    System.setProperty("maven.multiModuleProjectDirectory", ".");
    String[] params = paramsList.toArray(new String[]{});
    int state = mavenCli.doMain(params, "..", System.out, System.err);
    if(0 == state) setSpecNameAndId();

    return state;
  }

  private void setSpecNameAndId() throws IOException {
    String specPath = output + "/" + artifactId.toLowerCase() + "-connector/src/main/resources/spec.json";
    String specJson = FileUtils.readFileToString(new File(specPath), "utf8");
    JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    DataMap dataMap = jsonParser.fromJson(specJson);
    DataMap propertyMap = jsonParser.fromJson(jsonParser.toJson(dataMap.get("properties")));
    propertyMap.put("name",artifactId);
    propertyMap.put("id",artifactId.toLowerCase());
    dataMap.put("properties",propertyMap);
    String outputSpec = JSON.toJSONString(dataMap, SerializerFeature.PrettyFormat);
    FileUtils.writeStringToFile(new File(specPath),outputSpec,"utf-8");
  }
}
