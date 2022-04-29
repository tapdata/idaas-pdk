package io.tapdata.entity.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * class to describe relation of graph
 *
 * @author Jarad
 * @date 2022/4/28
 */
public class GraphRelation extends GraphResource {

    private GraphEntity srcEntity;

    private GraphEntity destEntity;

    private Set<GraphResource> relationTypes;

    private Integer rank;

    public GraphRelation(String idAndName) {
        this(idAndName, idAndName);
    }

    public GraphRelation(String id, String name) {
        super(id, name);
        relationTypes = new HashSet<>();
    }

    public static GraphRelation create(String idAndName) {
        return new GraphRelation(idAndName);
    }

    public static GraphRelation create(String id, String name) {
        return new GraphRelation(id, name);
    }

    public GraphEntity getSrcEntity() {
        return srcEntity;
    }

    public GraphRelation srcEntity(GraphEntity srcEntity) {
        this.srcEntity = srcEntity;
        return this;
    }

    public GraphEntity getDestEntity() {
        return destEntity;
    }

    public GraphRelation destEntity(GraphEntity destEntity) {
        this.destEntity = destEntity;
        return this;
    }

    public Set<GraphResource> getRelationTypes() {
        return relationTypes;
    }

    public GraphRelation relationTypes(Set<GraphResource> relationTypes) {
        this.relationTypes = relationTypes;
        return this;
    }

    public Integer getRank() {
        return rank;
    }

    public GraphRelation rank(Integer rank) {
        this.rank = rank;
        return this;
    }

    public GraphRelation addTypes(Collection<GraphResource> types) {
        relationTypes.addAll(types);
        return this;
    }

    public GraphRelation addType(GraphResource type) {
        relationTypes.add(type);
        return this;
    }
}
