import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.core.api.impl.JsonParserImpl;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@DisplayName("Tests for discover schema")
public class DiscoverSchemaTest {
    private final JsonParser jsonParser = new JsonParserImpl();

    @Test
    @Disabled("Disabled")
    @DisplayName("Test method discoverSchema")
    void discoverSchema() throws IOException {
        String sourcePath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\source.json";
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        DefaultMap source_map = mapper.readValue(new File(sourcePath), DefaultMap.class);
        TapTable tapTable = new TapTable("empty_table");
        for (Map.Entry<String, Object> entry : source_map.entrySet()) {
            DefaultMap defaultMap = jsonParser.fromJson(jsonParser.toJson(entry.getValue()));
            TapField tapField = new TapField(entry.getKey(), (String) defaultMap.get("originType"));
            tapTable.add(tapField);
        }
        // TODO TAPTYPE SCHEMA -> DORIS SCHEMA

    }

}