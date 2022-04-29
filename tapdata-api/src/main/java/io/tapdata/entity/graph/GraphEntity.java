package io.tapdata.entity.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * class to describe entity of graph
 *
 * @author Jarad
 * @date 2022/4/28
 */
public class GraphEntity extends GraphResource {

    private Set<GraphResource> entityLabels;

    public GraphEntity(String idAndName) {
        this(idAndName, idAndName);
    }

    public GraphEntity(String id, String name) {
        super(id, name);
        entityLabels = new HashSet<>();
    }

    public static GraphEntity create(String idAndName) {
        return new GraphEntity(idAndName);
    }

    public static GraphEntity create(String id, String name) {
        return new GraphEntity(id, name);
    }

    public Set<GraphResource> getEntityLabels() {
        return entityLabels;
    }

    public GraphEntity entityLabels(Set<GraphResource> entityLabels) {
        this.entityLabels = entityLabels;
        return this;
    }

    public GraphEntity addLabels(Collection<GraphResource> labels) {
        entityLabels.addAll(labels);
        return this;
    }

    public GraphEntity addLabel(GraphResource label) {
        entityLabels.add(label);
        return this;
    }
}
