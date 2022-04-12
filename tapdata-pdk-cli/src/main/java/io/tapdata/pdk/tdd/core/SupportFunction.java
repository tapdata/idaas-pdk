package io.tapdata.pdk.tdd.core;

import io.tapdata.pdk.apis.functions.connector.TapFunction;

public class SupportFunction {
    private Class<? extends TapFunction> function;
    private String errorMessage;

    public SupportFunction(Class<? extends TapFunction> function, String errorMessage) {
        this.function = function;
        this.errorMessage = errorMessage;
    }

    public Class<? extends TapFunction> getFunction() {
        return function;
    }

    public void setFunction(Class<? extends TapFunction> function) {
        this.function = function;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
