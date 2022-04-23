import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Administrator
 * @date 2022/4/19
 */
public class Main {

    public static void main(String[] args) throws Exception {
//        Connection conn = DriverManager.getConnection("jdbc:mysql://81.71.122.50:6521/guus", "root", "supercoolGJ0628@cpic");
        Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.1.189:2881/coolgj", "root", "rootPWD123");
//        List<TapTable> tapTableList = new LinkedList<>();
//        DatabaseMetaData databaseMetaData = conn.getMetaData();
//        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), "guus", null, new String[]{"TABLE"});
//        while (tableResult.next()) {
//            String tableName = tableResult.getString("TABLE_NAME");
//            ResultSet columnsResult = databaseMetaData.getColumns(conn.getCatalog(), "guus", tableName, null);
//            while (columnsResult.next()) {
//
//            }
//        }
        Statement statement = conn.createStatement(1004,1008);
        ResultSet rs = statement.executeQuery("select * from aaa");
        rs.first();
        Object o = rs.getObject("id");
        System.out.println(o.getClass());
    }

}
