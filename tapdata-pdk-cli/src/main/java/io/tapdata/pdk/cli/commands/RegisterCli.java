package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.cli.services.UploadFileService;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.InputStream;
import java.util.*;

@CommandLine.Command(
        description = "Push PDK jar file into Tapdata",
        subcommands = MainCli.class
)
public class RegisterCli extends CommonCli {
    private static final String TAG = RegisterCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "One ore more pdk jar files")
    File[] files;

    @CommandLine.Option(names = { "-a", "--auth" }, required = true, description = "Provide auth token to register")
    private String authToken;

    @CommandLine.Option(names = { "-t", "--tm" }, required = true, description = "Tapdata TM url")
    private String tmUrl;

    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        try {
            TapConnectorManager.getInstance().start(Arrays.asList(files));

            for(File file : files) {
              List<String> jsons = new ArrayList<>();
              TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
              Collection<TapNodeInfo> tapNodeInfoCollection = connector.getTapNodeClassFactory().getConnectorTapNodeInfos();
              Map<String, InputStream> inputStreamMap = new HashMap<>();
              for(TapNodeInfo nodeInfo : tapNodeInfoCollection) {
                TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
                String iconPath = specification.getIcon();
                if (StringUtils.isNotBlank(iconPath)) {
                  InputStream is = nodeInfo.readResource(iconPath);
                  if (is != null) {
                    inputStreamMap.put(iconPath, is);
                  }
                }
                JSONObject o = (JSONObject)JSON.toJSON(specification);
                o.put("type", nodeInfo.getNodeType());
                // get the version info and group info from jar
                o.put("version", nodeInfo.getNodeClass().getPackage().getImplementationVersion());
                o.put("group", nodeInfo.getNodeClass().getPackage().getImplementationVendor());
                String jsonString = o.toJSONString();
                jsons.add(jsonString);
              }
              System.out.println(tapNodeInfoCollection);
              UploadFileService.upload(inputStreamMap, file, jsons, tmUrl, authToken);
            }
        } catch (Throwable throwable) {
            CommonUtils.logError(TAG, "Start failed", throwable);
        }
        return 0;
    }
    public void validateAndFill(DAGDescriber dataFlowDescriber) {
//        Validator.checkAllNotNull(ErrorCodes.CLI_MISSING_SOURCE_OR_TARGET, dataFlowDescriber.getSourceNode(), dataFlowDescriber.getTargetNode());
//
//        SourceNode sourceNode = dataFlowDescriber.getSourceNode();
//        if(sourceNode.getId() == null) {
//            sourceNode.setId(CommonUtils.uuid());
//        }
//        if(sourceNode.getSourceOptions() == null) {
//            sourceNode.setSourceOptions(new SourceOptions());
//        }
//
//        TargetNode targetNode = dataFlowDescriber.getTargetNode();
//        if(targetNode.getId() == null) {
//            targetNode.setId(CommonUtils.uuid());
//        }
//        if(targetNode.getTargetOptions() == null) {
//            targetNode.setTargetOptions(new TargetOptions());
//        }
//
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_DATA_SOURCE_OR_TABLE, sourceNode.getDataSource(), sourceNode.getTable());
//
//        DataSource sourceDataSource = sourceNode.getDataSource();
//        if(sourceDataSource.getId() == null) {
//            sourceDataSource.setId(CommonUtils.uuid());
//        }
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_CONNECTION_STRING_OR_TYPE, sourceDataSource.getConnectionString(), sourceDataSource.getType());
//
//        Table sourceNodeTable = sourceNode.getTable();
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_DATABASE_OR_NAME, sourceNodeTable.getDatabase(), sourceNodeTable.getName());
//
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_DATA_SOURCE_OR_TABLE, targetNode.getDataSource(), targetNode.getTable());
//
//        DataSource targetDataSource = targetNode.getDataSource();
//        if(targetDataSource.getId() == null) {
//            targetDataSource.setId(CommonUtils.uuid());
//        }
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_CONNECTION_STRING_OR_TYPE, targetDataSource.getConnectionString(), targetDataSource.getType());
//
//        Table targetTable = targetNode.getTable();
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_DATABASE_OR_NAME, targetTable.getDatabase(), targetTable.getName());
//
//        ScriptNode scriptNode = dataFlowDescriber.getScriptNode();
//        if(scriptNode != null) {
//            if(scriptNode.getType() == null)
//                scriptNode.setType(ScriptConstants.Groovy);
//            if(scriptNode.getProcessMethod() == null)
//                scriptNode.setProcessMethod("process");
//            Validator.checkAllNotNull(ErrorCodes.CLI_SCRIPT_NODE_MISSING_CLASS_NAME_OR_ROOT_PATH, scriptNode.getClassName(), scriptNode.getRootPath());
//        }
//        logger.info("sourceNode {}", JSON.toJSONString(sourceNode, true));
//        logger.info("scriptNode {}", (scriptNode != null ? JSON.toJSONString(scriptNode, true) : "null"));
//        logger.info("targetNode {}", JSON.toJSONString(targetNode, true));
    }


}
