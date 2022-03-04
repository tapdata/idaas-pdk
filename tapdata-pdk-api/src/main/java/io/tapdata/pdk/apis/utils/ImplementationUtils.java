package io.tapdata.pdk.apis.utils;

import java.lang.reflect.Method;

public class ImplementationUtils {
    private static volatile Object classFactory;
    private static Method createMethod;
    private static TapUtils tapUtils;
    private static TypeConverter typeConverter;
    private static final Object lock = new int[0];

    private static Object create(Class<?> clazz) {
        if(classFactory == null) {
            synchronized (ImplementationUtils.class) {
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
            return createMethod.invoke(classFactory, clazz);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public static TapUtils getTapUtils() {
        if(tapUtils == null) {
            synchronized (lock) {
                if(tapUtils == null) {
                    tapUtils = (TapUtils) create(TapUtils.class);
                }
            }
        }
        return tapUtils;
    }

    public static TypeConverter getTypeConverter() {
        if(typeConverter == null) {
            synchronized (lock) {
                if(typeConverter == null) {
                    typeConverter = (TypeConverter) create(TypeConverter.class);
                }
            }
        }
        return typeConverter;
    }
}
