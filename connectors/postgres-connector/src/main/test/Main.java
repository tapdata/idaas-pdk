import java.sql.*;
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
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:5432/coolgj", "postgres", "abc123");
        PreparedStatement statement = conn.prepareStatement("insert into public.ccc values (?)");
        statement.setObject(1,"B");
        statement.addBatch();
        statement.setObject(1,"AC");
        statement.addBatch();
        statement.executeBatch();
//        statement.setObject(1, new java.sql.Timestamp(123123123123L));
//        statement.execute();
//        statement.close();
//        DatabaseMetaData databaseMetaData = conn.getMetaData();
//        ResultSet resultSet = databaseMetaData.getIndexInfo("coolgj", "public", "bbb", false, false);
//        resultSet.first();
//        while(resultSet.next()) {
//            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//            for(int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
//                System.out.println(resultSetMetaData.getColumnName(i+1) + ":" + resultSet.getObject(i+1));
//            }
//        }
        conn.close();
    }
}
