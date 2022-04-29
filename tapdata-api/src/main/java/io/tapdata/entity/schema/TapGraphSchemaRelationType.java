package io.tapdata.entity.schema;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * schema for relation type of graph
 *
 * @author Jarad
 * @date 2022/4/28
 */
public class TapGraphSchemaRelationType extends TapItem<TapField> {

    private LinkedHashMap<String, TapField> typeAttributesMap;

    private List<TapIndex> indexList;

    private String typeId;

    private String typeName;

    private String typeComment;
}
