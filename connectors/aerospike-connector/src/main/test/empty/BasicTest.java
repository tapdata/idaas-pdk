package empty;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

//@TapConnectorClass("source.json")
public class BasicTest {
    @BeforeClass
    public void init(){

    }

    @Test
    public void BasicConnection() {
        AerospikeClient client = new AerospikeClient("192.168.153.131", 3000);
        Assert.assertNotNull(client);
        WritePolicy policy = new WritePolicy();
        policy.timeoutDelay = 20;

        // insert multi key-record

        // for (int i = 0; i < 1 ; i ++){
        //     Key key = new Key("test", "test_set_name", "test_key_" + i);
        //     // insert
        //     for (int j = 0; j < 10; j++) {
        //         Bin b = new Bin("name" + j, "value" + j);
        //         client.put(policy, key, b);
        //     }
        // }

        Key key = new Key("test", "test_set_name", "test_key_0");
        // insert
        // default not store primary key
        // ref: https://stackoverflow.com/questions/52717748/how-to-get-the-primary-key-pk-of-a-record-in-aerospike
        Bin pk_b = new Bin("PK", "test_key_0");
        client.put(policy, key, pk_b);
        for (int j = 0; j < 10; j++) {
            Bin b = new Bin("name" + j, "value" + j);
            client.put(policy, key, b);
        }

        // select specified key
        // select all bin
        Record record = client.get(policy, key);
        Assert.assertEquals("{PK=test_key_0, name0=value0, name1=value1, name2=value2, name3=value3, name4=value4, name5=value5, name6=value6, name7=value7, name8=value8, name9=value9}", record.bins.toString());

        // select specified bin
        record = client.get(policy, key, "name0");
        Assert.assertEquals("{name0=value0}", record.bins.toString());


        // update - Same as insert
        // update specified bin
        Bin binString = new Bin("name0", "value-update0");
        client.put(policy, key, binString);
        record = client.get(policy, key, "name0");
        Assert.assertEquals("{name0=value-update0}", record.bins.toString());

        // delete
        // delete specified bin
        Bin delete_spec_bin = Bin.asNull("name0");
        client.put(policy, key, delete_spec_bin);
        record = client.get(policy, key, "name0");
        Assert.assertNull(record.bins);
        record = client.get(policy, key, "name1");
        Assert.assertEquals("{name1=value1}", record.bins.toString());

        // delete key
        client.delete(policy, key);
        record = client.get(policy, key);
        Assert.assertNull(record);

    }
}
