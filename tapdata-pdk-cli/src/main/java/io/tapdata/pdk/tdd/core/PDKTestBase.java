package io.tapdata.pdk.tdd.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.services.UploadFileService;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.*;

public abstract class PDKTestBase {
    private String jarFile;
    private static final String TAG = PDKTestBase.class.getSimpleName();

    public PDKTestBase() {
        jarFile = CommonUtils.getProperty("pdk_test_jar_file", "");
        if(StringUtils.isBlank(jarFile))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        File file = new File(jarFile);
        if(!file.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarFile + " is not a file or not exists");
        TapConnectorManager.getInstance().start(Arrays.asList(file));
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
            JSONObject o = (JSONObject) JSON.toJSON(specification);
            o.put("type", nodeInfo.getNodeType());
            String jsonString = o.toJSONString();
            jsons.add(jsonString);
        }
        PDKLogger.info(TAG, "tapNodeInfoCollection {}", tapNodeInfoCollection);
    }
}
