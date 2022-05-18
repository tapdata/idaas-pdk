import io.tapdata.connector.postgres.PostgresJdbcContext;
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
        PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        Connection conn = postgresJdbcContext.getConnection();
//        DatabaseMetaData databaseMetaData = postgresJdbcContext.getConnection().getMetaData();
//        ResultSet tableResult = databaseMetaData.getTables(postgresConfig.getDatabase(), postgresConfig.getSchema(), null, new String[]{"TABLE"});
//        tableResult.last();
        PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) FROM information_schema.table_privileges " +
                        "WHERE grantee=? AND table_catalog=? AND table_schema=? ");
        ps.setObject(1, postgresConfig.getUser());
        ps.setObject(2, postgresConfig.getDatabase());
        ps.setObject(3, postgresConfig.getSchema());
        postgresJdbcContext.query(ps, rs -> {
            rs.next();
            System.out.println(rs.getInt(1));
        });


//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
