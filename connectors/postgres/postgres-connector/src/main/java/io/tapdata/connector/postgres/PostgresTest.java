package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest implements AutoCloseable {

    private final PostgresConfig postgresConfig;
    private Connection conn;

    public PostgresTest(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        postgresConfig = PostgresConfig.load(connectionConfig);
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
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(postgresConfig.getDatabaseUrl(), postgresConfig.getUser(), postgresConfig.getPassword());
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test number of tables and privileges
    public TestItem testPrivilege() {
        try (
                PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        ) {
            AtomicInteger tablePrivileges = new AtomicInteger();
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM information_schema.table_privileges " +
                            "WHERE grantee=? AND table_catalog=? AND table_schema=? ");
            ps.setObject(1, postgresConfig.getUser());
            ps.setObject(2, postgresConfig.getDatabase());
            ps.setObject(3, postgresConfig.getSchema());
            postgresJdbcContext.query(ps, resultSet -> {
                resultSet.next();
                tablePrivileges.set(resultSet.getInt(1));
            });
            ResultSet privilegeResult = ps.executeQuery();
            privilegeResult.next();
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
            ResultSet resultSet = conn.createStatement().executeQuery(
                    "SELECT * FROM  pg_roles WHERE rolname='" + postgresConfig.getUser() + "'");
            resultSet.next();
            if (resultSet.getBoolean("rolreplication")) {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!");
            }
        } catch (SQLException e) {
            return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    private int tableCount() throws SQLException {
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), postgresConfig.getSchema(), null, new String[]{"TABLE"});
        tableResult.last();
        return tableResult.getRow();
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
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
