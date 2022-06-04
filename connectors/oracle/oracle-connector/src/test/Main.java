import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.oracle.OracleJdbcContext;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.utils.DataMap;

import java.sql.Connection;
import java.sql.ResultSet;

public class Main {
    public static void main(String[] args) throws Throwable {
        DataMap map = new DataMap();
        map.put("host", "192.168.1.126");
        map.put("port", 1521);
        map.put("database", "tap122c");
        map.put("schema", "TAPDATA");
        map.put("extParams", "");
        map.put("user", "tapdata");
        map.put("password", "Gotapd8!");
        OracleConfig oracleConfig = (OracleConfig) new OracleConfig().load(map);
//        postgresConfig.setHost("192.168.1.189");
//        postgresConfig.setPort(5432);
//        postgresConfig.setDatabase("COOLGJ");
//        postgresConfig.setSchema("public");
//        postgresConfig.setExtParams("");
//        postgresConfig.setUser("postgres");
//        postgresConfig.setPassword("gj0628");
        OracleJdbcContext oracleJdbcContext = (OracleJdbcContext) DataSourcePool.getJdbcContext(oracleConfig, OracleJdbcContext.class);
        Connection connection = oracleJdbcContext.getConnection();
        ResultSet rs = connection.createStatement().executeQuery("select * from \"A\"");
        while (rs.next()) {
            System.out.println(rs.getString("NAME"));
        }
        oracleJdbcContext.finish();

//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
