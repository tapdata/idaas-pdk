package io.tapdata.connector.postgres;

import io.tapdata.base.DataSourcePool;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest implements AutoCloseable {

    private final PostgresConfig postgresConfig;
    private final PostgresJdbcContext postgresJdbcContext;

    private final static String PG_ROLE_INFO = "SELECT * FROM pg_roles WHERE rolname='%s'";
    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";

    public PostgresTest(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig);
    }

    //Test host and port
    public TestItem testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(postgresConfig.getHost(), postgresConfig.getPort());
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test pg connect and log in
    public TestItem testConnect() {
        try (
                Connection connection = postgresJdbcContext.getConnection()
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test number of tables and privileges
    public TestItem testPrivilege() {
        try (
                Connection conn = postgresJdbcContext.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT count(*) FROM information_schema.table_privileges " +
                                "WHERE grantee=? AND table_catalog=? AND table_schema=? ")
        ) {
            AtomicInteger tablePrivileges = new AtomicInteger();
            ps.setObject(1, postgresConfig.getUser());
            ps.setObject(2, postgresConfig.getDatabase());
            ps.setObject(3, postgresConfig.getSchema());
            postgresJdbcContext.query(ps, resultSet -> tablePrivileges.set(resultSet.getInt(1)));
            if (tablePrivileges.get() >= 7 * tableCount()) {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no all privileges for some tables, Check it!");
            }
        } catch (Throwable e) {
            return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testReplication() {
        try {
            AtomicBoolean rolReplication = new AtomicBoolean();
            postgresJdbcContext.query(String.format(PG_ROLE_INFO, postgresConfig.getUser()),
                    resultSet -> rolReplication.set(resultSet.getBoolean("rolreplication")));
            if (rolReplication.get()) {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!");
            }
        } catch (Throwable e) {
            return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    private int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        postgresJdbcContext.query(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    @Override
    public void close() {
        try {
            postgresJdbcContext.finish();
        } catch (Exception ignored) {
        }
    }
}

enum PostgresTestItem {

    HOST_PORT("Check host port is invalid"),
    //    CHECK_VERSION("Check database version"),
    CHECK_CDC_PRIVILEGES("Check replication privileges"),
    CHECK_TABLE_PRIVILEGE("Check all for table privilege"),
    ;

    private final String content;

    PostgresTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
