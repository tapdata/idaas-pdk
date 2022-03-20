package empty;


import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.aerospike.bean.IRecord;
import io.tapdata.connector.aerospike.bean.TapAerospikeRecord;
import io.tapdata.connector.aerospike.utils.AerospikeSinkConfig;
import io.tapdata.connector.aerospike.utils.AerospikeStringSink;
import io.tapdata.entity.utils.DefaultMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ConnectionTestTest {
    private AerospikeSinkConfig sinkConfig;
    private final AerospikeStringSink aerospikeStringSink = new AerospikeStringSink();
    private WritePolicy policy = new WritePolicy();


    public boolean isConnected() {
        return aerospikeStringSink.client != null && aerospikeStringSink.client.isConnected();
    }

    public void initConnection(String configPath) throws IOException {
        if (isConnected()) {
            aerospikeStringSink.client.close();
        }
        sinkConfig = AerospikeSinkConfig.load(configPath);
        policy.timeoutDelay = 20;

        try {
            aerospikeStringSink.open(sinkConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearKeyInKeySet(AerospikeSinkConfig sinkConfig, String keyStr) throws Exception {
        if (!isConnected()) {
            throw new Exception("connection is not established");
        }
        Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
        aerospikeStringSink.client.delete(policy, key);
    }


    @Test
    public void BasicConnectionTest() throws IOException {
        String configPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\src\\main\\resources\\target.json";
        initConnection(configPath);
        Assert.assertTrue(aerospikeStringSink.client.isConnected());
        aerospikeStringSink.client.close();
        Assert.assertFalse(aerospikeStringSink.client.isConnected());
    }

    @Test
    public void WriteToAerospikeTest() throws Exception {
        String configPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\src\\main\\resources\\target.json";
        initConnection(configPath);

        String json = "{\"after\":{\"id\":1.0,\"description\":\"description123\",\"name\":\"name123\",\"age\":12.0},\"table\":{\"id\":\"empty-table1\",\"name\":\"empty-table1\",\"nameFieldMap\":{\"id\":{\"name\":\"id\",\"originType\":\"VARCHAR\",\"partitionKeyPos\":1,\"pos\":1,\"primaryKey\":true},\"description\":{\"name\":\"description\",\"originType\":\"TEXT\",\"pos\":2},\"name\":{\"name\":\"name\",\"originType\":\"VARCHAR\",\"pos\":3},\"age\":{\"name\":\"age\",\"originType\":\"DOUBLE\",\"pos\":4}}},\"time\":1647660346515}";
        DefaultMap defaultMap = JSON.parseObject(json, DefaultMap.class);
        JSONObject after_json_obj = (JSONObject) defaultMap.get("after");

        String keyStr = after_json_obj.get("id").toString();
        String after_json = after_json_obj.toJSONString();

        clearKeyInKeySet(sinkConfig, keyStr);
        IRecord<String> tapAerospikeRecord = new TapAerospikeRecord(after_json, keyStr);
        aerospikeStringSink.write(tapAerospikeRecord);

        Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
        Assert.assertEquals("{PK=1.0}",aerospikeStringSink.client.get(policy, key, "PK").bins.toString());
        Assert.assertEquals("{id=1.0}",aerospikeStringSink.client.get(policy, key, "id").bins.toString());
        Assert.assertEquals("{description=description123}",aerospikeStringSink.client.get(policy, key, "description").bins.toString());
        Assert.assertEquals("{name=name123}",aerospikeStringSink.client.get(policy, key, "name").bins.toString());
        Assert.assertEquals("{age=12.0}",aerospikeStringSink.client.get(policy, key, "age").bins.toString());

        clearKeyInKeySet(sinkConfig,keyStr);
        aerospikeStringSink.close();

    }
}
