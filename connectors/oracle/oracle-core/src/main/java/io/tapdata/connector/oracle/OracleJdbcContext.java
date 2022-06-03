package io.tapdata.connector.oracle;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.base.JdbcContext;
import io.tapdata.connector.oracle.config.OracleConfig;

public class OracleJdbcContext extends JdbcContext {

    private final static String TAG = OracleJdbcContext.class.getSimpleName();

    public OracleJdbcContext(OracleConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }
}
