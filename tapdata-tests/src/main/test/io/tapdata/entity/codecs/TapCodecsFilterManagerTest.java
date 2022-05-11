package io.tapdata.entity.codecs;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.value.TapValue;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TapCodecsFilterManagerTest {
    @Test
    public void test() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> map = map(
                entry("string", "string"),
                entry("int", 5555),
                entry("long", 34324L),
                entry("double", 343.324d)
                );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("string", field("string", "varchar").tapType(tapString().bytes(50L)));
        sourceNameFieldMap.put("int", field("int", "number(32)").tapType(tapNumber().bit(32)));
        sourceNameFieldMap.put("long", field("long", "number(64)").tapType(tapNumber().bit(64)));
        sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(3).bit(64)));

        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        Map<String, TapField> nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);

        map.put("dateTime", new Date());
        codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        assertNotNull(map.get("dateTime"));
        TapValue tapValue = (TapValue) map.get("dateTime");
        assertEquals(tapValue.getTapType().getClass().getSimpleName(), "TapDateTime");

        nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);
        assertNotNull(nameFieldMap.get("dateTime"));
        TapField dateTimeField = nameFieldMap.get("dateTime");
        assertEquals(dateTimeField.getTapType().getClass().getSimpleName(), "TapDateTime");
    }
}
