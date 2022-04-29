package io.tapdata.entity.graph;

/**
 * @author Administrator
 * @date 2022/4/28
 */
public class Main {

    public static void main(String[] args) {
        GraphEntity jarad = GraphEntity.create("Jarad")
                .addLabel(GraphResource.create("Person").addProperty("job", "it"))
                .addLabel(GraphResource.create("Animal").addProperty("sex", "male"));
        GraphEntity mavis = GraphEntity.create("Mavis")
                .addLabel(GraphResource.create("Person").addProperty("job", "free"))
                .addLabel(GraphResource.create("Animal").addProperty("sex", "female"));
        TapGraph graph = TapGraph.create(true).drawEntity(jarad).drawEntity(mavis);
        graph.drawRelation(GraphRelation.create("marryWith").srcEntity(jarad).destEntity(mavis).addType(GraphResource.create("marry").addProperty("year", 8)));
        System.out.println(graph);
    }
}
