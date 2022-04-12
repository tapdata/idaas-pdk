package io.tapdata.connector.aerospike.utils;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import io.tapdata.connector.aerospike.bean.IRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AerospikeStringSink {
    public AerospikeStringSink() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeStringSink.class);
    private AerospikeSinkConfig aerospikeSinkConfig;
    private WritePolicy writePolicy;
    public AerospikeClient client;

    public void open(AerospikeSinkConfig config) {
        this.aerospikeSinkConfig = config;
        if (this.aerospikeSinkConfig.getSeedHosts() != null && this.aerospikeSinkConfig.getKeyspace() != null) {
            this.writePolicy = new WritePolicy();
            this.writePolicy.maxRetries = this.aerospikeSinkConfig.getRetries();
            this.writePolicy.setTimeout(this.aerospikeSinkConfig.getTimeoutMs());
            this.createClient();
        } else {
            throw new IllegalArgumentException("Required property not set.");
        }
    }

    public boolean isConnected() {
        return this.client != null && this.client.isConnected();
    }

    public void close() {
        if (this.client != null) {
            this.client.close();
        }

        LOG.info("Connection Closed");
    }

    public Record read(String keySet, String keyStr) {
        Key key = new Key(this.aerospikeSinkConfig.getKeyspace(), keySet, keyStr);
        return client.get(writePolicy, key);
    }


    public void write(IRecord<String> record, String keySet) {
        Key key = new Key(this.aerospikeSinkConfig.getKeyspace(), keySet, record.getKey().get());
        if (record.getKey().isPresent()) {
            Bin key_bin = new Bin("PK", record.getKey().get());
            this.client.put(this.writePolicy, key, key_bin);
        }

        Bin[] bins = new Bin[record.getBinValuesMap().size()];
        int idx = 0;
        for (Map.Entry<String, Object> entry : record.getBinValuesMap().entrySet()) {
            String binKey = entry.getKey();
            if (binKey.length() > 14) {
                binKey = binKey.substring(0,14);
            }
            Bin bin = new Bin(binKey, entry.getValue());
            bins[idx++] = bin;
        }
        this.client.put(this.writePolicy, key, bins);
    }

    public void dropKeySet(String keySet){
        this.client.truncate(null,this.aerospikeSinkConfig.getKeyspace(),keySet,null);
    }

    private void createClient() {
        String[] hosts = this.aerospikeSinkConfig.getSeedHosts().split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid Seed Hosts");
        } else {
            Host[] aeroSpikeHosts = new Host[hosts.length];

            for (int i = 0; i < hosts.length; ++i) {
                String[] hostPort = hosts[i].split(":");
                aeroSpikeHosts[i] = new Host(hostPort[0], Integer.parseInt(hostPort[1]));
            }

            ClientPolicy policy = new ClientPolicy();
            if (this.aerospikeSinkConfig.getUserName() != null && !this.aerospikeSinkConfig.getUserName().isEmpty() && this.aerospikeSinkConfig.getPassword() != null && !this.aerospikeSinkConfig.getPassword().isEmpty()) {
                policy.user = this.aerospikeSinkConfig.getUserName();
                policy.password = this.aerospikeSinkConfig.getPassword();
            }

            this.client = new AerospikeClient(policy, aeroSpikeHosts);
        }
    }
}
