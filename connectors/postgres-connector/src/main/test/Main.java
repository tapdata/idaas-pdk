import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Administrator
 * @date 2022/4/29
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        List<String> a = Arrays.asList("a","b");
//        a.forEach(k -> {
//            if("a".equals(k))
//                return;
//            System.out.println(k);
//        });
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:5432/coolgj", "postgres", "abc123");
        PreparedStatement statement = conn.prepareStatement("insert into public.aaa values (?)");
        statement.setObject(1, new java.sql.Timestamp(123123123123L));
        statement.execute();
        statement.close();
        conn.close();
    }
}
