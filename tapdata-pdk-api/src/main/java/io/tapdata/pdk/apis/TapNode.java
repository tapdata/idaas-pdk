package io.tapdata.pdk.apis;

/**
 * Tapdata node in a DAG
 */
public interface TapNode {
    /**
     * Tapdata node closed in a DAG
     */
    void destroy();
}
