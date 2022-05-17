import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class Main2 {

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:5432/COOLGJ", "postgres", "gj0628");
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), "public", null, new String[]{"TABLE"});
        tableResult.last();
        System.out.println(tableResult.getRow());
        conn.close();
    }
}
