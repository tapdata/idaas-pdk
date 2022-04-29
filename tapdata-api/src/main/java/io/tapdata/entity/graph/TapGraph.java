package io.tapdata.entity.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * class to describe graph
 *
 * @author Administrator
 * @date 2022/4/28
 */
public class TapGraph {

    private boolean freeSchema = true;

    private final Set<GraphEntity> graphEntities;

    private final Set<GraphRelation> graphRelations;

    public TapGraph() {
        graphEntities = new HashSet<>();
        graphRelations = new HashSet<>();
    }

    public TapGraph(boolean freeSchema) {
        this();
        this.freeSchema = freeSchema;
    }

    public static TapGraph create(boolean freeSchema) {
        return new TapGraph(freeSchema);
    }

    public TapGraph drawEntities(Collection<GraphEntity> entities) {
        graphEntities.addAll(entities);
        return this;
    }

    public TapGraph drawRelations(Collection<GraphRelation> relations) {
        graphRelations.addAll(relations);
        return this;
    }

    public TapGraph drawEntity(GraphEntity entity) {
        graphEntities.add(entity);
        return this;
    }

    public TapGraph drawRelation(GraphRelation relation) {
        graphRelations.add(relation);
        return this;
    }

    public boolean isFreeSchema() {
        return freeSchema;
    }

    public void setFreeSchema(boolean freeSchema) {
        this.freeSchema = freeSchema;
    }

    public Set<GraphEntity> getGraphEntities() {
        return graphEntities;
    }

    public Set<GraphRelation> getGraphRelations() {
        return graphRelations;
    }
}
