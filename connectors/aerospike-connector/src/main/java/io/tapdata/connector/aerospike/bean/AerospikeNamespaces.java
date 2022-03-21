package io.tapdata.connector.aerospike.bean;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class AerospikeNamespaces {
    private Map<String, ArrayList<AerospikeSet>> namespaceSetsMap;

    private  AerospikeNamespaces(){}

    public AerospikeNamespaces(AerospikeClient client) {
        this.namespaceSetsMap = new LinkedHashMap<>();
        String[] namespaces = Info.request(client.getInfoPolicyDefault(), client.getNodes()[0], "namespaces").split(";");
        for (String ns: namespaces) {
            this.namespaceSetsMap.put(ns,new ArrayList<>());
        }
        String[] raw_sets = Info.request(client.getInfoPolicyDefault(), client.getNodes()[0], "sets").split(";");

        for (String raw_set : raw_sets) {
            String[] set_info = raw_set.split(":");
            String namespace = set_info[0].split("=")[1];
            String setName = set_info[1].split("=")[1];
            ArrayList<AerospikeSet> setsList = namespaceSetsMap.get(namespace);
            setsList.add(new AerospikeSet(namespace, setName));
        }

    }

    public String[] getNamespaces(){
        if(this.namespaceSetsMap == null) return null;
        return this.namespaceSetsMap.keySet().toArray(new String[0]);
    }

    public ArrayList<AerospikeSet> getSets(String namespaces){
        if(this.namespaceSetsMap == null) return null;
        return this.namespaceSetsMap.getOrDefault(namespaces,null);
    }
}
