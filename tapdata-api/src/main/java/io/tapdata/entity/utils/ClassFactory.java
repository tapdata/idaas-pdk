package io.tapdata.entity.utils;

import java.lang.reflect.Method;

public class ClassFactory {
    private static volatile Object classFactory;
    private static Method createMethod;
    private static final Object lock = new int[0];
    private ClassFactory() {}

    public static <T> T create(Class<T> clazz) {
        if(classFactory == null) {
            synchronized (ClassFactory.class) {
                if(classFactory == null) {
                    String classFactoryStr = "io.tapdata.pdk.core.runtime.TapRuntime";
                    try {
                        Class<?> clazz1 = Class.forName(classFactoryStr);
                        Method method = clazz1.getDeclaredMethod("getInstance");
                        Object tapRuntime = method.invoke(null);
                        Method factoryMethod = clazz1.getDeclaredMethod("getImplementationClassFactory");
                        classFactory = factoryMethod.invoke(tapRuntime);
                        createMethod = classFactory.getClass().getDeclaredMethod("create", Class.class);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        try {
            return (T) createMethod.invoke(classFactory, clazz);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

}
