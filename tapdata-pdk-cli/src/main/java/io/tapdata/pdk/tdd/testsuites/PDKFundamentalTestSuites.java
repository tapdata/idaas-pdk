package io.tapdata.pdk.tdd.testsuites;

import io.tapdata.pdk.tdd.tests.ConnectionTestTest;
import io.tapdata.pdk.tdd.tests.DiscoverSchemaTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;


@SelectClasses({ConnectionTestTest.class})
@Suite
@SuiteDisplayName("Fundamental Test")
public class PDKFundamentalTestSuites {
}
