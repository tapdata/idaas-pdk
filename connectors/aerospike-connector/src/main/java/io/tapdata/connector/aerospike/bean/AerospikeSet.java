package io.tapdata.connector.aerospike.bean;

public class AerospikeSet {
    private String namespace;
    private String setName;
    private String objects;
    private String tombstones;
    private String memory_data_bytes;
    private String device_data_bytes;
    private String truncate_lut;
    private String sindexes;
    private String indexPopulating;
    private String disableEviction;
    private String enableIndex;
    private String stopWritesCount;

    public AerospikeSet(String namespace, String setName) {
        this.namespace = namespace;
        this.setName = setName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSetName() {
        return setName;
    }

    public void setSetName(String setName) {
        this.setName = setName;
    }

    @Override
    public String toString() {
        return "AerospikeSet{" +
                "namespace='" + namespace + '\'' +
                ", setName='" + setName + '\'' +
                '}';
    }
}
