package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.utils.InstanceFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TargetTypesGeneratorImplTest {
    private TargetTypesGenerator targetTypesGenerator;

    @BeforeEach
    void beforeEach() {
        targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
    }

    @Test
    void convert() {

    }
}
