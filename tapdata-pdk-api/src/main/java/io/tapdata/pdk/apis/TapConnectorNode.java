package io.tapdata.pdk.apis;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;

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
    void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer);

    /**
     * Test connection
     * @param databaseContext
     * @return
     */
    void connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer);

}
