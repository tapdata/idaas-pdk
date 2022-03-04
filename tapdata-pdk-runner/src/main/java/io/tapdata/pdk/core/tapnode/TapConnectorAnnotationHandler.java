package io.tapdata.pdk.core.tapnode;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.core.error.CoreException;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TapConnectorAnnotationHandler extends TapBaseAnnotationHandler {
    private static final String TAG = TapConnectorAnnotationHandler.class.getSimpleName();
    public TapConnectorAnnotationHandler() {
        super();
    }

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            newerIdGroupTapNodeInfoMap = new ConcurrentHashMap<>();
            PDKLogger.info(TAG, "--------------TapConnector Classes Start-------------");
            for(Class<?> clazz : classes) {
                TapConnector tapConnector = clazz.getAnnotation(TapConnector.class);
                if(tapConnector != null) {
                    URL url = clazz.getClassLoader().getResource(tapConnector.value());
                    if(url != null) {
                        TapNodeSpecification tapNodeSpecification = null;
                        try {
                            InputStream is = url.openStream();
                            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
                            TapNodeContainer tapNodeContainer = JSON.parseObject(json, TapNodeContainer.class);
                            tapNodeSpecification = tapNodeContainer.getSpecification();
                            String errorMessage = null;
                            if(tapNodeSpecification == null)
                                errorMessage = "Specification not found";
                            if(errorMessage == null)
                                errorMessage = tapNodeSpecification.verify();
                             if(errorMessage != null) {
                                PDKLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}", tapConnector.value(), json, errorMessage);
                                continue;
                            }
                            tapNodeSpecification.setApplications(tapNodeContainer.getApplications());
                            String connectorType = findConnectorType(clazz);
                            if(connectorType == null) {
                                PDKLogger.error(TAG, "Connector class for id {} title {} only have TapConnector annotation, but not implement either TapSource or TapTarget which is must, {} will be ignored...", tapNodeSpecification.idAndGroup(), tapNodeSpecification.getName(), clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if(tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                PDKLogger.info(TAG, "Found new connector {} type {} version {} buildNumber {}", tapNodeSpecification.idAndGroup(), connectorType, tapNodeSpecification.getVersion(), tapNodeSpecification.getBuildNumber());
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                if(specification.getBuildNumber() < tapNodeSpecification.getBuildNumber()) {
                                    tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                    tapNodeInfo.setNodeType(connectorType);
                                    tapNodeInfo.setNodeClass(clazz);
                                    PDKLogger.warn(TAG, "Found newer connector {} type {} version {} buildNumber {} replaced buildNumber {}", tapNodeSpecification.idAndGroup(), connectorType, tapNodeSpecification.getVersion(), tapNodeSpecification.getBuildNumber(), specification.getBuildNumber());
                                } else {
                                    PDKLogger.warn(TAG, "Older connector will be ignored, connector {} type {} version {} buildNumber {}", specification.idAndGroup(), tapNodeInfo.getNodeType(), specification.getVersion(), specification.getBuildNumber());
                                }
                            }
                        } catch(Throwable throwable) {
                            PDKLogger.error(TAG, "Handle tap node specification failed, path {} error {}", tapConnector.value(), throwable.getMessage());
                        }
                    } else {
                        PDKLogger.error(TAG, "Resource {} doesn't be found, connector class {} will be ignored", tapConnector.value(), clazz);
                    }
                }
            }
            PDKLogger.info(TAG, "--------------TapConnector Classes End-------------");
        }
    }

    private String findConnectorType(Class<?> clazz) {
        boolean isSource = false;
        boolean isTarget = false;
        if(TapSource.class.isAssignableFrom(clazz)) {
            isSource = true;
        }
        if(TapTarget.class.isAssignableFrom(clazz)) {
            isTarget = true;
        }
        if(isSource && isTarget) {
            return TapNodeInfo.NODE_TYPE_SOURCE_TARGET;
        } else if(isSource) {
            return TapNodeInfo.NODE_TYPE_SOURCE;
        } else if(isTarget) {
            return TapNodeInfo.NODE_TYPE_TARGET;
        }
        return null;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapConnector.class;
    }

}
