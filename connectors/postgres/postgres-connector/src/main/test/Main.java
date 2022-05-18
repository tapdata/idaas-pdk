import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    public static void main (String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:5432/coolgj", "postgres", "abc123");
        DatabaseMetaData databaseMetaData = conn.getMetaData();
//        ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), postgresConfig.getSchema(), tableName, null);
    }
}
