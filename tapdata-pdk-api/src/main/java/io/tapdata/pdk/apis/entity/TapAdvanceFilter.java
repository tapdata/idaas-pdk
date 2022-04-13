package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;

/**
 * >, >=, <, <=
 * or, and
 */
public class TapAdvanceFilter extends TapFilter {
    private Integer limit;
    private List<QueryOperator> operators;
    private List<SortOn> sortOnList;

    public static TapAdvanceFilter create() {
        return new TapAdvanceFilter();
    }

    public TapAdvanceFilter limit(int limit) {
        this.limit = limit;
        return this;
    }

    public TapAdvanceFilter op(QueryOperator operator) {
        if(operators == null) {
            operators = new ArrayList<>();
        }
        operators.add(operator);
        return this;
    }

    public TapAdvanceFilter match(DataMap match) {
        this.match = match;
        return this;
    }

    public TapAdvanceFilter sort(SortOn sortOn) {
        if(sortOnList == null) {
            sortOnList = new ArrayList<>();
        }
        sortOnList.add(sortOn);
        return this;
    }

    public List<QueryOperator> getOperators() {
        return operators;
    }

    public void setOperators(List<QueryOperator> operators) {
        this.operators = operators;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public List<SortOn> getSortOnList() {
        return sortOnList;
    }

    public void setSortOnList(List<SortOn> sortOnList) {
        this.sortOnList = sortOnList;
    }
}
