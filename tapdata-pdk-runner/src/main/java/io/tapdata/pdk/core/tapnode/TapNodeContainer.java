package io.tapdata.pdk.core.tapnode;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;

public class TapNodeContainer {
    private TapNodeSpecification specification;

    private Map<String, Object> applications;

    public Map<String, Object> getApplications() {
        return applications;
    }

    public void setApplications(Map<String, Object> applications) {
        this.applications = applications;
    }

    public TapNodeSpecification getSpecification() {
        return specification;
    }

    public void setSpecification(TapNodeSpecification specification) {
        this.specification = specification;
    }

}
