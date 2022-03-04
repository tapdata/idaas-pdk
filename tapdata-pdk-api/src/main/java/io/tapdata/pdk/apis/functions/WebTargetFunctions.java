package io.tapdata.pdk.apis.functions;


import io.tapdata.pdk.apis.functions.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.target.simple.DeleteRecordFunction;
import io.tapdata.pdk.apis.functions.target.simple.InsertRecordFunction;
import io.tapdata.pdk.apis.functions.target.simple.UpdateRecordFunction;
import io.tapdata.pdk.apis.functions.target.simple.UpsertRecordFunction;

public class WebTargetFunctions extends CommonFunctions<WebTargetFunctions> {
    private DeleteRecordFunction deleteRecordFunction;
    private InsertRecordFunction insertRecordFunction;
    private UpdateRecordFunction updateRecordFunction;
    private UpsertRecordFunction upsertRecordFunction;
    private QueryByFilterFunction queryByFilterFunction;


    public WebTargetFunctions withDeleteRecordFunction(DeleteRecordFunction function) {
        deleteRecordFunction = function;
        return this;
    }

    public WebTargetFunctions withInsertRecordFunction(InsertRecordFunction function) {
        insertRecordFunction = function;
        return this;
    }

    public WebTargetFunctions withQueryByFilterFunction(QueryByFilterFunction function) {
        queryByFilterFunction = function;
        return this;
    }

    public WebTargetFunctions withUpdateRecordFunction(UpdateRecordFunction function) {
        updateRecordFunction = function;
        return this;
    }

    public WebTargetFunctions withUpsertRecordFunction(UpsertRecordFunction function) {
        upsertRecordFunction = function;
        return this;
    }

    public DeleteRecordFunction getDeleteRecordFunction() {
        return deleteRecordFunction;
    }

    public InsertRecordFunction getInsertRecordFunction() {
        return insertRecordFunction;
    }

    public QueryByFilterFunction getQueryByFilterFunction() {
        return queryByFilterFunction;
    }

    public UpdateRecordFunction getUpdateRecordFunction() {
        return updateRecordFunction;
    }

    public UpsertRecordFunction getUpsertRecordFunction() {
        return upsertRecordFunction;
    }
}
