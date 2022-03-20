package io.tapdata.pdk.apis.context;
import io.tapdata.entity.utils.DefaultConcurrentMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;


public class TapContext {
    protected TapNodeSpecification specification;

    protected final DefaultConcurrentMap attributes = new DefaultConcurrentMap();

    public TapContext(TapNodeSpecification specification) {
        this.specification = specification;
    }

    public Object putAttributeIfAbsent(String key, Object value) {
        return attributes.putIfAbsent(key, value);
    }

    public Object putAttribute(String key, Object value) {
        return attributes.put(key, value);
    }

    public Object removeAttribute(String key) {
        return attributes.remove(key);
    }

    public TapNodeSpecification getSpecification() {
        return specification;
    }

    public void setSpecification(TapNodeSpecification specification) {
        this.specification = specification;
    }

}
