import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.PostgresTest;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.kit.DbKit;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    public static void main (String[] args) throws Throwable {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("192.168.1.189");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("COOLGJ");
        postgresConfig.setSchema("public");
        postgresConfig.setExtParams("");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        postgresJdbcContext.query("SELECT * FROM information_schema.tables WHERE table_catalog='COOLGJ' AND table_schema='public' AND table_name='DMLTest_postgres_BSwgs' ORDER BY table_name",
                resultSet-> {
            System.out.println(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                });
        postgresJdbcContext.close();

//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
