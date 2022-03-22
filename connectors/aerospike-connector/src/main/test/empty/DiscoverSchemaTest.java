package empty;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.tapdata.connector.aerospike.bean.AerospikeSet;
import io.tapdata.connector.aerospike.utils.AerospikeSinkConfig;
import io.tapdata.connector.aerospike.utils.AerospikeStringSink;
import io.tapdata.connector.aerospike.bean.AerospikeNamespaces;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class DiscoverSchemaTest {
    private final String configPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\src\\main\\resources\\target.json";
    private AerospikeSinkConfig sinkConfig;
    private final AerospikeStringSink aerospikeStringSink = new AerospikeStringSink();
    private WritePolicy policy = new WritePolicy();


    public boolean isConnected() {
        return aerospikeStringSink.client != null && aerospikeStringSink.client.isConnected();
    }

    public void initConnection(String configPath) throws Exception {
        if (isConnected()) {
            aerospikeStringSink.client.close();
        }
        sinkConfig = AerospikeSinkConfig.load(configPath);
        policy.timeoutDelay = 20;
        aerospikeStringSink.open(sinkConfig);
    }

    @Test
    public void discoverSchemaTest() throws Exception {
        initConnection(configPath);

        // test get namespaces
        AerospikeClient client = this.aerospikeStringSink.client;
        String[] ns =AerospikeNamespaces.getNamespaces(client);
        Assert.assertEquals("test", ns[0]);
        Assert.assertEquals("bar", ns[1]);

        // first set
        String keySet = "test_set_name";
        client.truncate(client.getInfoPolicyDefault(), sinkConfig.getKeyspace(), keySet, null);
        String keyStr = "ket_str";
        Key key = new Key(sinkConfig.getKeyspace(), keySet, keyStr);
        Bin b = new Bin("PK", keyStr);
        client.put(policy, key, b);

        // second set
        keySet = "test_new_set";
        client.truncate(client.getInfoPolicyDefault(), sinkConfig.getKeyspace(), keySet, null);
        keyStr = "ket_str";
        key = new Key(sinkConfig.getKeyspace(), keySet, keyStr);
        b = new Bin("PK", keyStr);
        client.put(policy, key, b);

        // test get sets
        ArrayList<AerospikeSet> sets = AerospikeNamespaces.getSets(aerospikeStringSink.client, sinkConfig.getKeyspace());
        Assert.assertNotNull(sets);
        Assert.assertEquals("test_set_name", sets.get(0).getSetName());
        Assert.assertEquals("test_new_set", sets.get(1).getSetName());
    }

}
