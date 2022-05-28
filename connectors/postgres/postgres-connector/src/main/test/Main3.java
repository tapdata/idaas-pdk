import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Date;

public class Main3 {
    public static void main(String[] args) throws Throwable {
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
        PreparedStatement preparedStatement = connection.prepareStatement(
                "DELETE FROM \"PgTest1234\" WHERE (  ? is null)");
        preparedStatement.setNull(1, Types.DATE);
//        preparedStatement.setObject(2,new java.sql.Date(new Date().getTime()));
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        postgresJdbcContext.close();

//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
