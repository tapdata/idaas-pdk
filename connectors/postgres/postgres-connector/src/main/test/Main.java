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
        Connection connection = postgresJdbcContext.getConnection();
        connection.createStatement().execute("create table cool(aaa varchar)");
        PreparedStatement preparedStatement = connection.prepareStatement("insert into cool values (?)");
        preparedStatement.setObject(1, "111");
        preparedStatement.addBatch();
        preparedStatement.setObject(1, "222");
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        preparedStatement = connection.prepareStatement("update cool set aaa=? where aaa=?");
        preparedStatement.setObject(1, "333");
        preparedStatement.setObject(2, "111");
        preparedStatement.execute();
        connection.commit();
        postgresJdbcContext.close();

//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
