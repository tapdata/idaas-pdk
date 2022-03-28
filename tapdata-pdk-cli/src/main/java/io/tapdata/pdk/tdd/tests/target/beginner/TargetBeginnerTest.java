package io.tapdata.pdk.tdd.tests.target.beginner;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;

@DisplayName("Tests for target beginner test")
public class TargetBeginnerTest extends PDKTestBase {
    private static final String TAG = TargetBeginnerTest.class.getSimpleName();
    @Test
    @DisplayName("Test method handleDML")
    void targetTest() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {

        }, TapNodeInfo.NODE_TYPE_TARGET, TapNodeInfo.NODE_TYPE_SOURCE_TARGET);
    }
}
