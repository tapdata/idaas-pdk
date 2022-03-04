package io.tapdata.pdk.apis.functions;


import io.tapdata.pdk.apis.functions.target.simple.*;

public class SimpleTargetFunctions extends CommonFunctions<SimpleTargetFunctions> {

    private DeleteRecordFunction deleteRecordFunction;
    private InsertRecordFunction insertRecordFunction;
    private UpdateRecordFunction updateRecordFunction;
    private UpsertRecordFunction upsertRecordFunction;
    private WriteSchemaFunction writeSchemaFunction;
    private WriteTableFunction writeTableFunction;

    public SimpleTargetFunctions withDeleteRecordFunction(DeleteRecordFunction function) {
        deleteRecordFunction = function;
        return this;
    }

    public SimpleTargetFunctions withInsertRecordFunction(InsertRecordFunction function) {
        insertRecordFunction = function;
        return this;
    }

    public SimpleTargetFunctions withUpdateRecordFunction(UpdateRecordFunction function) {
        updateRecordFunction = function;
        return this;
    }

    public SimpleTargetFunctions withUpsertRecordFunction(UpsertRecordFunction function) {
        upsertRecordFunction = function;
        return this;
    }

    public SimpleTargetFunctions withWriteSchemaFunction(WriteSchemaFunction function) {
        writeSchemaFunction = function;
        return this;
    }

    public SimpleTargetFunctions withWriteTableFunction(WriteTableFunction function) {
        writeTableFunction = function;
        return this;
    }

    public DeleteRecordFunction getDeleteRecordFunction() {
        return deleteRecordFunction;
    }

    public void setDeleteRecordFunction(DeleteRecordFunction deleteRecordFunction) {
        this.deleteRecordFunction = deleteRecordFunction;
    }

    public InsertRecordFunction getInsertRecordFunction() {
        return insertRecordFunction;
    }

    public void setInsertRecordFunction(InsertRecordFunction insertRecordFunction) {
        this.insertRecordFunction = insertRecordFunction;
    }

    public UpdateRecordFunction getUpdateRecordFunction() {
        return updateRecordFunction;
    }

    public void setUpdateRecordFunction(UpdateRecordFunction updateRecordFunction) {
        this.updateRecordFunction = updateRecordFunction;
    }

    public UpsertRecordFunction getUpsertRecordFunction() {
        return upsertRecordFunction;
    }

    public void setUpsertRecordFunction(UpsertRecordFunction upsertRecordFunction) {
        this.upsertRecordFunction = upsertRecordFunction;
    }

    public WriteSchemaFunction getWriteSchemaFunction() {
        return writeSchemaFunction;
    }

    public void setWriteSchemaFunction(WriteSchemaFunction writeSchemaFunction) {
        this.writeSchemaFunction = writeSchemaFunction;
    }

    public WriteTableFunction getWriteTableFunction() {
        return writeTableFunction;
    }

    public void setWriteTableFunction(WriteTableFunction writeTableFunction) {
        this.writeTableFunction = writeTableFunction;
    }
}
