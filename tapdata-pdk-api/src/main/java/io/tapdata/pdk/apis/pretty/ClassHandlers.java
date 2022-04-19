package io.tapdata.pdk.apis.pretty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClassHandlers {
    private static final String TAG = ClassHandlers.class.getSimpleName();

    private final Map<Class<?>, List<ClassObjectHandler>> classHandlersMap = new ConcurrentHashMap<>();
    public <T> void register(Class<T> tClass, ClassObjectHandler<T> objectHandler) {
        List<ClassObjectHandler> objectHandlers = classHandlersMap.compute(tClass, (aClass, classObjectHandlers) -> new ArrayList<>());
        if(!objectHandlers.contains(objectHandler)) {
            objectHandlers.add(objectHandler);
        }
    }

    public void handle(Object t) {
        if(t != null) {
            List<ClassObjectHandler> objectHandlers = classHandlersMap.get(t.getClass());
            if(objectHandlers != null) {
                for(ClassObjectHandler objectHandler : objectHandlers) {
                    objectHandler.handle(t);
                }
            } /*else {
                PDKLogger.error(TAG, "Class {} not found corresponding handler, maybe forget to register one? ", t.getClass());
            }*/
        }
    }
}
