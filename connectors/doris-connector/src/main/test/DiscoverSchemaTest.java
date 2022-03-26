import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.doris.utils.DorisConfig;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.core.api.impl.JsonParserImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@DisplayName("Tests for discover schema")
public class DiscoverSchemaTest {
    private final JsonParser jsonParser = new JsonParserImpl();
    private DorisConfig dorisConfig;
    private Connection conn;
    private Statement stmt;
    private TapTable tapTable;
    private String tableName = "empty";


    private void initConnection() throws Exception {
        if (conn == null) {
            String targetPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\target.json";
            dorisConfig = DorisConfig.load(targetPath);
            String dbUrl = dorisConfig.getDatabaseUrl();
            conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
            stmt = conn.createStatement();
        }
    }

    private void initTapTable(String tableName) throws IOException {
        if (tapTable != null) return;
        String sourcePath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\source.json";
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        DataMap source_map = mapper.readValue(new File(sourcePath), DataMap.class);
        tapTable = new TapTable(tableName);
        for (Map.Entry<String, Object> entry : source_map.entrySet()) {
            DataMap dataMap = jsonParser.fromJson(jsonParser.toJson(entry.getValue()));
            TapField tapField = new TapField(entry.getKey(), (String) dataMap.get("originType"));
            if (dataMap.get("partitionKeyPos") != null) {
                tapField.setPartitionKeyPos((Integer) dataMap.get("partitionKeyPos"));
            }
            if (dataMap.get("pos") != null) {
                tapField.setPos((Integer) dataMap.get("partitionKeyPos"));
            }
            if (dataMap.get("primaryKey") != null) {
                tapField.isPrimaryKey((Boolean) dataMap.get("primaryKey"));
            }
            tapTable.add(tapField);
        }
    }

    private String buildColumnDefinition(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getOriginType() == null) continue;
            builder.append(tapField.getName()).append(' ');
            builder.append(tapField.getOriginType()).append(' ');
            if (tapField.getNullable() != null && !tapField.getNullable()) {
                builder.append("NOT NULL").append(' ');
            } else {
                builder.append("NULL").append(' ');
            }
            if (tapField.getDefaultValue() != null) {
                builder.append("DEFAULT").append(' ').append(tapField.getDefaultValue()).append(' ');
            }
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private String buildColumnValues(TapTable tapTable, DataMap record) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        StringBuilder builder = new StringBuilder();
        for (String columnName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(columnName);
            if (tapField.getOriginType() == null) continue;
            Object value = record.getValue(columnName, null);
            if (value == null) {
                if (tapField.getNullable()) builder.append("NULL").append(',');
                else builder.append('"').append(tapField.getDefaultValue()).append('"').append(',');
            } else builder.append('"').append(value).append('"').append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private String buildDistributedKey(Collection<String> primaryKeyNames) {
        StringBuilder builder = new StringBuilder();
        for (String fieldName : primaryKeyNames) {
            builder.append(fieldName);
            builder.append(',');
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }


    @Test
    @Disabled("Disabled")
    @DisplayName("Test drop createTable")
    void dropTable() throws Exception {
        initConnection();
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(stmt);
        initTapTable(tableName);

        ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tableName, new String[]{"TABLE"});
        if (table.first()) {
            String sql = "DROP TABLE " + tableName;
            stmt.execute(sql);
        }
        table = conn.getMetaData().getTables(conn.getCatalog(), null, tableName, new String[]{"TABLE"});
        Assertions.assertFalse(table.first());
    }

    @Test
    @Disabled("Disabled")
    @DisplayName("Test method createTable")
    void createTable() throws Exception {
        initConnection();
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(stmt);
        initTapTable(tableName);

        // TODO TAPTYPE SCHEMA -> DORIS SCHEMA
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String sql = "CREATE TABLE " + tapTable.getName() +
                "(" + this.buildColumnDefinition(tapTable) + ")" +
                "DISTRIBUTED BY HASH(" + this.buildDistributedKey(primaryKeys) + ") BUCKETS 10 " +
                "PROPERTIES(\"replication_num\" = \"1\")";
        stmt.execute(sql);
    }


    @Test
    @Disabled("Disabled")
    @DisplayName("Test method truncateTable")
    void truncateTable() throws Exception {
        initConnection();
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(stmt);
        initTapTable(tableName);

        ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tableName, new String[]{"TABLE"});
        if (table.first()) {
            String sql = "TRUNCATE TABLE " + tapTable.getName();
            stmt.execute(sql);
        }
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    @Disabled("Disabled")
    @DisplayName("Test method insertIntoTable")
    void insertIntoTable() throws Exception {
        initConnection();
        Assertions.assertNotNull(conn);
        Assertions.assertNotNull(stmt);
        initTapTable(tableName);

        String json = "{\"after\":{\"id\":1.0,\"description\":\"description123\",\"name\":\"name123\",\"age\":12.0},\"table\":{\"id\":\"empty-table1\",\"name\":\"empty-table1\",\"nameFieldMap\":{\"id\":{\"name\":\"id\",\"originType\":\"VARCHAR\",\"partitionKeyPos\":1,\"pos\":1,\"primaryKey\":true},\"description\":{\"name\":\"description\",\"originType\":\"TEXT\",\"pos\":2},\"name\":{\"name\":\"name\",\"originType\":\"VARCHAR\",\"pos\":3},\"age\":{\"name\":\"age\",\"originType\":\"DOUBLE\",\"pos\":4}}},\"time\":1647660346515}";
        DataMap dataMap = jsonParser.fromJson(json);
        DataMap after_map = jsonParser.fromJson(dataMap.get("after").toString());

        ResultSet table = conn.getMetaData().getTables(null, dorisConfig.getDatabase(), tableName, new String[]{"TABLE"});
        if (table.first()) {
            String sql = "INSERT INTO " + tapTable.getName() + " VALUES (" + this.buildColumnValues(tapTable, after_map) + ")";
            System.out.println(sql);
            stmt.execute(sql);
        }

        ResultSet resultSet = stmt.executeQuery("select * from " + tableName);
        Assertions.assertTrue(resultSet.next());
    }
}
