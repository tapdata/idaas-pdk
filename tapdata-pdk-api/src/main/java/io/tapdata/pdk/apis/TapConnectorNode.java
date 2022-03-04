package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;

import java.util.List;
import java.util.function.Consumer;

/**
 * Tapdata connector node.
 * Can be TapSource or TapTarget. Stand for a data source, not a processor.
 */
public interface TapConnectorNode extends TapNode  {
    /**
     * Return all tables in a database by TapListConsumer.
     *  @param connectionContext
     * @param consumer
     */
    void discoverSchema(TapConnectionContext connectionContext, TapListConsumer<TapTableOptions> consumer);

    /**
     * Test connection
     * @param databaseContext
     * @return
     */
    ConnectionTestResult connectionTest(TapConnectionContext databaseContext);

}
