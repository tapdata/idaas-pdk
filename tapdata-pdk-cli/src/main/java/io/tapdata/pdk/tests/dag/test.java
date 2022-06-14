package io.tapdata.pdk.tests.dag;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.*;
import io.tapdata.entity.logger.TapLogger;

import javax.annotation.Nonnull;
import java.lang.annotation.Target;

public class test {
    public static void main(String... args) throws Throwable {
//        System.out.println(convertJarFileName("mongodb-connector-v1.0-SNAPSHOT__628daf0716419763bbdce3f1__.jar"));
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);
        config.setInstanceName("Aplomb's InstanceName");
        config.setProperty("hazelcast.logging.type", "none");
//        SerializerConfig hazelcastCacheStatsSerializer = new SerializerConfig().setImplementation(new HazelcastCacheStatsSerializer()).setTypeClass(HazelcastCacheStats.class);
//        SerializerConfig hazelcastDataFlowCacheConfigSerializer = new SerializerConfig().setImplementation(new HazelcastDataFlowCacheConfigSerializer()).setTypeClass(DataFlowCacheConfig.class);
//        config.getSerializationConfig().addSerializerConfig(hazelcastCacheStatsSerializer);
//        config.getSerializationConfig().addSerializerConfig(hazelcastDataFlowCacheConfigSerializer);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        JetService jetService = instance.getJet();
        DAG dag = new DAG();
        Vertex s1 = new Vertex("s1", (SupplierEx<Processor>) SourceProcessor::new).localParallelism(1);
        Vertex p1 = new Vertex("p1", (SupplierEx<Processor>) ProcessorProcessor::new).localParallelism(1);
        Vertex t1 = new Vertex("t1", (SupplierEx<Processor>) TargetProcessor::new).localParallelism(1);
        dag.vertex(s1).vertex(p1).vertex(t1);
        EdgeConfig edgeConfig = new EdgeConfig().setQueueSize(2);
        dag.edge(Edge.between(s1, p1).setConfig(edgeConfig)).edge(Edge.between(p1, t1).setConfig(edgeConfig));
//        dag.edge(Edge.between(s1, t1));
        TapLogger.info("DAG", "{}", dag.toDotString());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("aplomb's job");
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        jetService.newJob(dag, jobConfig);
    }

}
