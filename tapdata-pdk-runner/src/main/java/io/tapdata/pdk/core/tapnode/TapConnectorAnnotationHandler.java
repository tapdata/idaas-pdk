package io.tapdata.pdk.core.tapnode;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
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
        if (classes != null) {
            newerIdGroupTapNodeInfoMap = new ConcurrentHashMap<>();
            PDKLogger.info(TAG, "--------------TapConnector Classes Start------------- size {}", classes.size());
            for (Class<?> clazz : classes) {
                TapConnectorClass tapConnectorClass = clazz.getAnnotation(TapConnectorClass.class);
                if (tapConnectorClass != null) {
                    URL url = clazz.getClassLoader().getResource(tapConnectorClass.value());
                    if (url != null) {
                        TapNodeSpecification tapNodeSpecification = null;
                        try {
                            InputStream is = url.openStream();
                            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
                            TapNodeContainer tapNodeContainer = JSON.parseObject(json, TapNodeContainer.class);
                            tapNodeSpecification = tapNodeContainer.getSpecification();

                            String errorMessage = null;
                            if (tapNodeSpecification == null) {
                                errorMessage = "Specification not found";
                            } else {
                                if(tapNodeSpecification.getGroup() == null) {
                                    tapNodeSpecification.setGroup(clazz.getPackage().getImplementationVendor());
                                }
                                if(tapNodeSpecification.getVersion() == null) {
                                    tapNodeSpecification.setVersion(clazz.getPackage().getImplementationVersion());
                                }
                            }

                            if (errorMessage == null)
                                errorMessage = tapNodeSpecification.verify();
                            if (errorMessage != null) {
                                PDKLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}", tapConnectorClass.value(), json, errorMessage);
                                continue;
                            }

                            tapNodeSpecification.setApplications(tapNodeContainer.getApplications());
                            String connectorType = findConnectorType(clazz);
                            if (connectorType == null) {
                                PDKLogger.error(TAG, "Connector class for id {} title {} only have TapConnector annotation, but not implement either TapSource or TapTarget which is must, {} will be ignored...", tapNodeSpecification.idAndGroup(), tapNodeSpecification.getName(), clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if (tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                PDKLogger.info(TAG, "Found new connector {} type {}", tapNodeSpecification.idAndGroup(), connectorType);
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                tapNodeInfo.setTapNodeSpecification(specification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                PDKLogger.warn(TAG, "Found newer connector {} type {}", tapNodeSpecification.idAndGroup(), connectorType);
                            }
                        } catch (Throwable throwable) {
                            PDKLogger.error(TAG, "Handle tap node specification failed, path {} error {}", tapConnectorClass.value(), throwable.getMessage());
                        }
                    } else {
                        PDKLogger.error(TAG, "Resource {} doesn't be found, connector class {} will be ignored", tapConnectorClass.value(), clazz);
                    }
                }
            }
            PDKLogger.info(TAG, "--------------TapConnector Classes End-------------");
        }
    }

    private String findConnectorType(Class<?> clazz) {
        boolean isSource = false;
        boolean isTarget = false;
        if (TapConnector.class.isAssignableFrom(clazz)) {
            try {
                TapConnector connector = (TapConnector) clazz.getConstructor().newInstance();
                ConnectorFunctions connectorFunctions = new ConnectorFunctions();
                TapCodecRegistry codecRegistry = new TapCodecRegistry();
                connector.registerCapabilities(connectorFunctions, codecRegistry);

                if (connectorFunctions.getBatchReadFunction() != null || connectorFunctions.getStreamReadFunction() != null) {
                    isSource = true;
                }
                if (connectorFunctions.getDmlFunction() != null) {
                    isTarget = true;
                }
            } catch (Throwable e) {
                e.printStackTrace();
                PDKLogger.error(TAG, "Find connector type failed, {} clazz {} will be ignored", e.getMessage(), clazz);
            }
        }

        if (isSource && isTarget) {
            return TapNodeInfo.NODE_TYPE_SOURCE_TARGET;
        } else if (isSource) {
            return TapNodeInfo.NODE_TYPE_SOURCE;
        } else if (isTarget) {
            return TapNodeInfo.NODE_TYPE_TARGET;
        }
        return null;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapConnectorClass.class;
    }

}
