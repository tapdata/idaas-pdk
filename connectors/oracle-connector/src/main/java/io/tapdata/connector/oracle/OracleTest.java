package io.tapdata.connector.oracle;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.oracle.config.OracleConfig;

public class OracleTest extends CommonDbTest {

    public OracleTest(OracleConfig oracleConfig) {
        super(oracleConfig);
        jdbcContext = DataSourcePool.getJdbcContext(oracleConfig, OracleJdbcContext.class, uuid);
    }

}
