package io.tapdata.pdk.apis.functions;


import io.tapdata.pdk.apis.functions.target.*;

public class TargetFunctions extends CommonFunctions<TargetFunctions> {

    private DMLFunction dmlFunction;
    private QueryByFilterFunction queryByFilterFunction;
    private DDLFunction ddlFunction;
    private TransactionFunction transactionFunction;



    public TargetFunctions withDMLFunction(DMLFunction function) {
        dmlFunction = function;
        return this;
    }

    public TargetFunctions withQueryByFilterFunction(QueryByFilterFunction function) {
        queryByFilterFunction = function;
        return this;
    }

    public TargetFunctions withDDLFunction(DDLFunction function) {
        ddlFunction = function;
        return this;
    }

    public TargetFunctions withTransactionFunction(TransactionFunction function) {
        transactionFunction = function;
        return this;
    }

    public DMLFunction getDmlFunction() {
        return dmlFunction;
    }

    public void setDmlFunction(DMLFunction dmlFunction) {
        this.dmlFunction = dmlFunction;
    }

    public QueryByFilterFunction getQueryByFilterFunction() {
        return queryByFilterFunction;
    }

    public void setQueryByFilterFunction(QueryByFilterFunction queryByFilterFunction) {
        this.queryByFilterFunction = queryByFilterFunction;
    }

    public DDLFunction getDdlFunction() {
        return ddlFunction;
    }

    public void setDdlFunction(DDLFunction ddlFunction) {
        this.ddlFunction = ddlFunction;
    }

    public TransactionFunction getTransactionFunction() {
        return transactionFunction;
    }

    public void setTransactionFunction(TransactionFunction transactionFunction) {
        this.transactionFunction = transactionFunction;
    }
}
