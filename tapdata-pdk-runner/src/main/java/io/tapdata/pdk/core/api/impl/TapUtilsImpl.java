package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.utils.TapUtils;
import io.tapdata.pdk.core.annotations.Implementation;
import io.tapdata.pdk.core.api.SourceNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.ReflectionUtil;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Implementation(TapUtils.class)
public class TapUtilsImpl implements TapUtils {

    @Override
    public void interval(Runnable runnable, int seconds) {
        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(runnable, seconds, TimeUnit.SECONDS);
    }

    public Object clone(Object obj) {
        if(obj instanceof Map) {
            Map<Object, Object> cloneMap = null;
            Map<?, ?> map = (Map<?, ?>) obj;
            if(ReflectionUtil.canBeInitiated(obj.getClass())) {
                try {
                    cloneMap = (Map<Object, Object>) ReflectionUtil.newInstance(obj.getClass(), null);
                } catch (Throwable e) {
                    e.printStackTrace();
                    return null;
                }
            } else {
                cloneMap = new LinkedHashMap<>();
            }
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                cloneMap.put(entry.getKey(), clone(entry.getValue()));
            }
            return cloneMap;
        } else if(obj instanceof Collection) {
            Collection<?> list = (Collection<?>) obj;
            Collection<Object> cloneList = null;
            if(ReflectionUtil.canBeInitiated(obj.getClass())) {
                try {
                    cloneList = (Collection<Object>) ReflectionUtil.newInstance(obj.getClass(), null);
                } catch (Throwable e) {
                    e.printStackTrace();
                    return null;
                }
            } else {
                cloneList = new ArrayList<>();
            }

            for(Object o : list) {
                cloneList.add(clone(o));
            }
            return cloneList;
        } else {
            return cloneObject(obj);
        }
    }


    private Object cloneObject(Object obj){
        try{
            if(ReflectionUtil.isPrimitiveOrWrapper(obj.getClass()) || obj instanceof String) {
                return obj;
            }
            if(obj instanceof Date) {
                Date date = (Date) obj;
                return new Date(date.getTime());
            }
            if(!ReflectionUtil.canBeInitiated(obj.getClass())) {
                return null;
            }
            Object clone = obj.getClass().newInstance();
            Field[] fields = ReflectionUtil.getFields(obj.getClass());
            for (Field field : fields) {
                field.setAccessible(true);
                Object fieldValue = field.get(obj);
                if(fieldValue == null || Modifier.isFinal(field.getModifiers())){
                    continue;
                }
                if(Map.class.isAssignableFrom(field.getType()) || Collection.class.isAssignableFrom(field.getType())) {
                    field.set(clone, clone(fieldValue));
                } else if(field.getType().isPrimitive() || field.getType().equals(String.class)
                        || field.getType().getSuperclass().equals(Number.class)
                        || field.getType().equals(Boolean.class)){
                    field.set(clone, fieldValue);
                } else {
                    if(fieldValue == obj){
                        field.set(clone, clone);
                    } else {
                        if(ReflectionUtil.canBeInitiated(field.getType())) {
                            field.set(clone, cloneObject(fieldValue));
                        }
                    }
                }
            }
            return clone;
        } catch(Throwable e){
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String... args) {
        Date date = new Date();
        System.out.println("date " + date);

        TapDAGNode node = new TapDAGNode();
        node.setChildNodeIds(Arrays.asList("1", "3"));
        DefaultMap defaultMap = new DefaultMap();

        TapTable table1 = new TapTable()
                .add(new TapField().name("f").originType("aaa"))
                .add(new TapField().name("a").originType("aa"));
        defaultMap.put("aaa", 1);
//        defaultMap.put("table", table1);
        node.setConnectionConfig(defaultMap);
        TapTable table = new TapTable()
                .add(new TapField().name("f").originType("aaa"))
                .add(new TapField().name("a").originType("aa"));
        node.setTable(table);
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        insertRecordEvent.setAfter(new HashMap<String, Object>(){{
            put("aa", "bb");
            put("cc", date);
//            put("ccca", node);
        }});
        insertRecordEvent.setInfo(new HashMap<String, Object>(){ {
            put("aaa", Arrays.asList("1", "2"));
            put("bbb", 1);
        }});
        insertRecordEvent.setPdkId("aaaa");
        insertRecordEvent.setTable(table);

        TapInsertRecordEvent clone = (TapInsertRecordEvent) new TapUtilsImpl().clone(insertRecordEvent);
        TapInsertRecordEvent insertRecordEvent1 = JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);

        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            new TapUtilsImpl().clone(insertRecordEvent);
        }
        System.out.println("reflection count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));
//        System.out.println("takes "  + (System.currentTimeMillis() - time));


        time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);
        }
        System.out.println("json count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));


//        time = System.currentTimeMillis();
//        for(int i = 0; i < 1000000; i++) {
//            JSON.parseObject(JSON.toJSONString(insertRecordEvent), TapInsertRecordEvent.class);
//        }
//        System.out.println("jackson count per second "  + (1000000d / ((System.currentTimeMillis() - time) / 1000d)));

    }
}
