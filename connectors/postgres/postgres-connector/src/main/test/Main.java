import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.PostgresTest;
import io.tapdata.connector.postgres.config.PostgresConfig;

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
        PostgresTest postgresTest = new PostgresTest(postgresConfig);
        System.out.println(postgresTest.testHostPort().toString());
        System.out.println(postgresTest.testConnect().toString());
        System.out.println(postgresTest.testPrivilege().toString());
        System.out.println(postgresTest.testReplication().toString());
        postgresTest.close();

//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
