package io.tapdata.connector.aerospike.bean;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;

import java.util.*;

public class AerospikeNamespaces {

    private  AerospikeNamespaces(){}


    public static String[] getNamespaces(AerospikeClient client){
        return Info.request(client.getInfoPolicyDefault(), client.getNodes()[0], "namespaces").split(";");
    }

    public static  ArrayList<AerospikeSet> getSets(AerospikeClient client,String namespace){
        String[] namespaces = getNamespaces(client);
        if(!Arrays.asList(namespaces).contains(namespace)) return  null;
        String[] raw_sets = Info.request(client.getInfoPolicyDefault(), client.getNodes()[0], "sets").split(";");
        ArrayList<AerospikeSet> setsList = new ArrayList<>();
        for (String raw_set : raw_sets) {
            if(Objects.equals(raw_set, "")) continue;
            String[] set_info = raw_set.split(":");
            if(set_info[0].split("=")[1].equals(namespace)){
                String setName = set_info[1].split("=")[1];
                setsList.add(new AerospikeSet(namespace, setName));
            }
        }
        return setsList;
    }
}
