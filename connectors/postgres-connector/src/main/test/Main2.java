import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class Main2 {

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:7475/COOLGJ", "postgres", "gj0628");
        PreparedStatement statement = conn.prepareStatement("ALTER TABLE ? REPLICA IDENTITY DEFAULT");
        statement.setObject(1, "Student");
        statement.execute();
        conn.close();
    }
}
