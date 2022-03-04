package io.tapdata.pdk.core.tapnode;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapRecordProcessor;
import io.tapdata.pdk.apis.annotations.TapProcessor;
import io.tapdata.pdk.core.error.CoreException;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TapProcessorAnnotationHandler extends TapBaseAnnotationHandler {
    private static final String TAG = TapProcessorAnnotationHandler.class.getSimpleName();
    public TapProcessorAnnotationHandler() {
        super();
    }

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null) {
            newerIdGroupTapNodeInfoMap = new ConcurrentHashMap<>();
            PDKLogger.info(TAG, "--------------TapProcessor Classes Start------------- {}", classes.size());
            for(Class<?> clazz : classes) {
                TapProcessor tapProcessor = clazz.getAnnotation(TapProcessor.class);
                if(tapProcessor != null) {
                    URL url = clazz.getClassLoader().getResource(tapProcessor.value());
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
                                PDKLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}", tapProcessor.value(), json, errorMessage);
                                continue;
                            }
                            String connectorType = findConnectorType(clazz);
                            if(connectorType == null) {
                                PDKLogger.error(TAG, "Processor class for id {} title {} only have TapProcessor annotation, but not implement TapProcessor which is must, {} will be ignored...", tapNodeSpecification.getId(), tapNodeSpecification.getName(), clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if(tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                PDKLogger.info(TAG, "Found new processor {} type {} version {} buildNumber {}", tapNodeSpecification.idAndGroup(), connectorType, tapNodeSpecification.getVersion(), tapNodeSpecification.getBuildNumber());
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                if(specification.getBuildNumber() < tapNodeSpecification.getBuildNumber()) {
                                    tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                    tapNodeInfo.setNodeType(connectorType);
                                    tapNodeInfo.setNodeClass(clazz);
                                    PDKLogger.warn(TAG, "Found newer processor {} type {} version {} buildNumber {} replaced buildNumber {}", tapNodeSpecification.idAndGroup(), connectorType, tapNodeSpecification.getVersion(), tapNodeSpecification.getBuildNumber(), specification.getBuildNumber());
                                } else {
                                    PDKLogger.warn(TAG, "Older processor will be ignored, processor {} type {} version {} buildNumber {}", specification.idAndGroup(), tapNodeInfo.getNodeType(), specification.getVersion(), specification.getBuildNumber());
                                }
                            }
                        } catch(Throwable throwable) {
                            PDKLogger.error(TAG, "Handle tap node specification failed, path {} error {}", tapProcessor.value(), throwable.getMessage());
                        }
                    } else {
                        PDKLogger.error(TAG, "Resource {} doesn't be found, processor class {} will be ignored", tapProcessor.value(), clazz);
                    }
                }
            }
            PDKLogger.info(TAG, "--------------TapProcessor Classes End-------------");
        }
    }

    private String findConnectorType(Class<?> clazz) {
        boolean isProcessor = false;
        if(TapRecordProcessor.class.isAssignableFrom(clazz)) {
            isProcessor = true;
        }
        if(isProcessor) {
            return TapNodeInfo.NODE_TYPE_PROCESSOR;
        }
        return null;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapProcessor.class;
    }

}
