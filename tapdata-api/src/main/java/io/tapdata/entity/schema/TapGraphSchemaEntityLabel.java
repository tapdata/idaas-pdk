package io.tapdata.entity.schema;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * schema for entity label of graph
 *
 * @author Jarad
 * @date 2022/4/28
 */
public class TapGraphSchemaEntityLabel extends TapItem<TapField> {

    private LinkedHashMap<String, TapField> tagAttributesMap;

    private List<TapIndex> indexList;

    private String labelId;

    private String labelName;

    private String labelComment;
}
