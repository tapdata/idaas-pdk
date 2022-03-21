package empty;

import com.aerospike.client.AerospikeClient;
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
        AerospikeNamespaces ns = new AerospikeNamespaces(client);
        Assert.assertEquals("test", ns.getNamespaces()[0]);
        Assert.assertEquals("bar", ns.getNamespaces()[1]);

        // test get sets
        ArrayList<AerospikeSet> sets = ns.getSets("test");
        Assert.assertEquals("test_set_name", sets.get(0).getSetName());
        Assert.assertEquals("test_new_set", sets.get(1).getSetName());
    }

}
