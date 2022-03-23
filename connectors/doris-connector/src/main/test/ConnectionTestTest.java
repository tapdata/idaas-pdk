import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.doris.utils.DorisConfig;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.utils.ImplementationUtils;
import io.tapdata.pdk.apis.utils.TapUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;


import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

@DisplayName("Tests for connection test")
public class ConnectionTestTest {
    private DorisConfig dorisConfig;
    private final TapUtils tapUtils = ImplementationUtils.getTapUtils();

    @Test
    @DisplayName("Test method connectionTest")
    void connectionTest() throws IOException, SQLException {
//        String sourcePath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\source.json";
//        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
//        DefaultMap source_map = mapper.readValue(new File(sourcePath), DefaultMap.class);

        String targetPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\target.json";
        dorisConfig = DorisConfig.load(targetPath);
        PreparedStatement stmt = null;
        String dbUrl = dorisConfig.getDatabaseUrl();
        Connection conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
        Assertions.assertNotNull(conn);


        String json = "{\"after\":{\"id\":1.0,\"description\":\"description123\",\"name\":\"name123\",\"age\":12.0},\"table\":{\"id\":\"empty-table1\",\"name\":\"empty-table1\",\"nameFieldMap\":{\"id\":{\"name\":\"id\",\"originType\":\"VARCHAR\",\"partitionKeyPos\":1,\"pos\":1,\"primaryKey\":true},\"description\":{\"name\":\"description\",\"originType\":\"TEXT\",\"pos\":2},\"name\":{\"name\":\"name\",\"originType\":\"VARCHAR\",\"pos\":3},\"age\":{\"name\":\"age\",\"originType\":\"DOUBLE\",\"pos\":4}}},\"time\":1647660346515}";
        DefaultMap defaultMap = tapUtils.fromJson(json);
        DefaultMap after_map = tapUtils.fromJson(defaultMap.get("after").toString());


        try {
            ResultSet tables = conn.getMetaData().getTables(conn.getCatalog(), null, dorisConfig.getTable(), new String[]{"TABLE"});

            while (tables.next()) {
                Assertions.assertEquals(dorisConfig.getTable(), tables.getString("TABLE_NAME"));
            }
            Class.forName(dorisConfig.getJdbcDriver());

            String insert = "insert into " + dorisConfig.getTable() + " values(?, ?, ?, ?)";
            stmt = conn.prepareStatement(insert);
            for (int i = 0; i < dorisConfig.getInsertBatchSize(); i++) {
                stmt.setString(1, after_map.getValue("id", null).toString());
                stmt.setString(2, after_map.getValue("description", null));
                stmt.setString(3, after_map.getValue("name", null));
                BigDecimal age = after_map.getValue("age", null);
                stmt.setDouble(4, age.doubleValue());
                stmt.addBatch();
            }
            int[] res = stmt.executeBatch();
            Assertions.assertEquals(dorisConfig.getInsertBatchSize(), res.length);

            String query = "select * from " + dorisConfig.getTable();
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Assertions.assertEquals(rs.getString(1), "1.0");
                Assertions.assertEquals(rs.getString(2), "description123");
                Assertions.assertEquals(rs.getString(3), "name123");
                Assertions.assertEquals(rs.getDouble(4), 12.0);
            }

            String truncate = "truncate table " + dorisConfig.getTable();
            stmt = conn.prepareStatement(truncate);
            stmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

    }
}
