package io.tapdata.connector.aerospike.utils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import io.tapdata.connector.aerospike.bean.IRecord;
import io.tapdata.connector.aerospike.bean.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AerospikeAbstractSink<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeAbstractSink.class);
    private AerospikeSinkConfig aerospikeSinkConfig;
    public AerospikeClient client;
    private WritePolicy writePolicy;
    private BlockingQueue<AerospikeAbstractSink<K, V>.AWriteListener> queue;
    private NioEventLoops eventLoops;
    private EventLoop eventLoop;

    public AerospikeAbstractSink() {
    }

    public void open(AerospikeSinkConfig config) throws Exception {
        this.aerospikeSinkConfig = config;
        // TODO this.aerospikeSinkConfig.getColumnName() != null 每次都需要指定列?
        if (this.aerospikeSinkConfig.getSeedHosts() != null && this.aerospikeSinkConfig.getKeyspace() != null && this.aerospikeSinkConfig.getColumnName() != null) {
            this.writePolicy = new WritePolicy();
            this.writePolicy.maxRetries = this.aerospikeSinkConfig.getRetries();
            this.writePolicy.setTimeout(this.aerospikeSinkConfig.getTimeoutMs());
            this.createClient();
            this.queue = new LinkedBlockingDeque(this.aerospikeSinkConfig.getMaxConcurrentRequests());

            for(int i = 0; i < this.aerospikeSinkConfig.getMaxConcurrentRequests(); ++i) {
                this.queue.put(new AerospikeAbstractSink.AWriteListener(this.queue));
            }

            this.eventLoops = new NioEventLoops(new EventPolicy(), 1);
            this.eventLoop = this.eventLoops.next();
        } else {
            throw new IllegalArgumentException("Required property not set.");
        }
    }

    public void close() throws Exception {
        if (this.client != null) {
            this.client.close();
        }

        if (this.eventLoops != null) {
            this.eventLoops.close();
        }

        LOG.info("Connection Closed");
    }

    public void write(IRecord<String> record) {
        /*TODO
           对于key record bin 的关系有一些疑问
           在pulasr的实现中只能指定column(bin)后进行写入，IRecord中record相当于column value，即按列写
           对于empty生成的json的数据是一行一行的，所以在IRecord中使用binValuesMap记录键值对信息，再进行写入，即按行写
        */
        KeyValue<K, V> keyValue = this.extractKeyValue(record);
        Key key = new Key(this.aerospikeSinkConfig.getKeyspace(), this.aerospikeSinkConfig.getKeySet(), keyValue.getKey().toString());

//        Bin bin = new Bin(this.aerospikeSinkConfig.getColumnName(), keyValue.getValue());
        AerospikeAbstractSink.AWriteListener listener = null;

        try {
            listener = (AerospikeAbstractSink.AWriteListener)this.queue.take();
        } catch (InterruptedException var7) {
            record.fail();
            return;
        }

        listener.setContext(record);

        Bin key_bin = new Bin("PK", keyValue.getKey());
        this.client.put(this.writePolicy,key, key_bin);
        for(Map.Entry<String, String> entry: record.getBinValuesMap().entrySet()){
            Bin bin = new Bin(entry.getKey(), String.valueOf(entry.getValue()));
            this.client.put(this.writePolicy,key,bin);
        }
//        this.client.put(this.writePolicy,key, bin);

//        this.client.put(this.eventLoop, listener, this.writePolicy, key, new Bin[]{bin});
    }

    private void createClient() {
        String[] hosts = this.aerospikeSinkConfig.getSeedHosts().split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid Seed Hosts");
        } else {
            Host[] aeroSpikeHosts = new Host[hosts.length];

            for(int i = 0; i < hosts.length; ++i) {
                String[] hostPort = hosts[i].split(":");
                aeroSpikeHosts[i] = new Host(hostPort[0], Integer.valueOf(hostPort[1]));
            }

            ClientPolicy policy = new ClientPolicy();
            if (this.aerospikeSinkConfig.getUserName() != null && !this.aerospikeSinkConfig.getUserName().isEmpty() && this.aerospikeSinkConfig.getPassword() != null && !this.aerospikeSinkConfig.getPassword().isEmpty()) {
                policy.user = this.aerospikeSinkConfig.getUserName();
                policy.password = this.aerospikeSinkConfig.getPassword();
            }

            this.client = new AerospikeClient(policy, aeroSpikeHosts);
        }
    }

    public abstract KeyValue<K, V> extractKeyValue(IRecord<String> var1);

    private class AWriteListener implements WriteListener {
        private IRecord<byte[]> context;
        private BlockingQueue<AerospikeAbstractSink<K, V>.AWriteListener> queue;

        public AWriteListener(BlockingQueue<AerospikeAbstractSink<K, V>.AWriteListener> queue) {
            this.queue = queue;
        }

        public void setContext(IRecord<byte[]> IRecord) {
            this.context = IRecord;
        }

        public void onSuccess(Key key) {
            if (this.context != null) {
                this.context.ack();
            }

            try {
                this.queue.put(this);
            } catch (InterruptedException var3) {
                throw new RuntimeException("Interrupted while being added to the queue", var3);
            }
        }

        public void onFailure(AerospikeException e) {
            if (this.context != null) {
                this.context.fail();
            }

            try {
                this.queue.put(this);
            } catch (InterruptedException var3) {
                throw new RuntimeException("Interrupted while being added to the queue", var3);
            }
        }
    }
}

