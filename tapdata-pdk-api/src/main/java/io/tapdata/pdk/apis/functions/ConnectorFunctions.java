package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.*;

public class ConnectorFunctions extends CommonFunctions<ConnectorFunctions> {
    private BatchReadFunction batchReadFunction;
    private StreamReadFunction streamReadFunction;
    private BatchCountFunction batchCountFunction;
    private BatchOffsetFunction batchOffsetFunction;
    private StreamOffsetFunction streamOffsetFunction;
    private DMLFunction dmlFunction;
    private QueryByFilterFunction queryByFilterFunction;
    private TransactionFunction transactionFunction;
    private CreateTableFunction createTableFunction;
    private AlterTableFunction alterTableFunction;
    private ClearTableFunction clearTableFunction;
    private DropTableFunction dropTableFunction;
    /**
     * TODO Not integrated yet.
     *
     * @param function
     * @return
     */
    public ConnectorFunctions supportBatchOffset(BatchOffsetFunction function) {
        batchOffsetFunction = function;
        return this;
    }

    /**
     * TODO Not integrated yet.
     *
     * @param function
     * @return
     */
    public ConnectorFunctions supportStreamOffset(StreamOffsetFunction function) {
        streamOffsetFunction = function;
        return this;
    }

    public ConnectorFunctions supportBatchRead(BatchReadFunction function) {
        batchReadFunction = function;
        return this;
    }

    public ConnectorFunctions supportStreamRead(StreamReadFunction function) {
        streamReadFunction = function;
        return this;
    }

    public ConnectorFunctions supportBatchCount(BatchCountFunction function) {
        this.batchCountFunction = function;
        return this;
    }

    public ConnectorFunctions supportDML(DMLFunction function) {
        this.dmlFunction = function;
        return this;
    }

    public ConnectorFunctions supportCreateTable(CreateTableFunction function) {
        this.createTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportAlterTable(AlterTableFunction function) {
        this.alterTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportClearTable(ClearTableFunction function) {
        this.clearTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportDropTable(DropTableFunction function) {
        this.dropTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportQueryByFilter(QueryByFilterFunction function) {
        this.queryByFilterFunction = function;
        return this;
    }

    public BatchReadFunction getBatchReadFunction() {
        return batchReadFunction;
    }

    public StreamReadFunction getStreamReadFunction() {
        return streamReadFunction;
    }

    public BatchCountFunction getBatchCountFunction() {
        return batchCountFunction;
    }

    public BatchOffsetFunction getBatchOffsetFunction() {
        return batchOffsetFunction;
    }

    public StreamOffsetFunction getStreamOffsetFunction() {
        return streamOffsetFunction;
    }

    public DMLFunction getDmlFunction() {
        return dmlFunction;
    }

    public QueryByFilterFunction getQueryByFilterFunction() {
        return queryByFilterFunction;
    }

    public TransactionFunction getTransactionFunction() {
        return transactionFunction;
    }

    public CreateTableFunction getCreateTableFunction() {
        return createTableFunction;
    }

    public AlterTableFunction getAlterTableFunction() {
        return alterTableFunction;
    }

    public ClearTableFunction getClearTableFunction() {
        return clearTableFunction;
    }

    public DropTableFunction getDropTableFunction() {
        return dropTableFunction;
    }
}
