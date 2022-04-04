package io.tapdata.entity.schema;


import io.tapdata.entity.type.TapType;

import java.io.Serializable;

public class TapField implements Serializable {
    public TapField() {}

    public TapField(String name, String originType) {
        this.name = name;
        this.originType = originType;
    }

    private String originType;
    public TapField originType(String originType) {
        this.originType = originType;
        return this;
    }
    /**
     * Field value can be null
     */
    private Boolean nullable;
    public TapField nullable(Boolean nullable) {
        this.nullable = nullable;
        return this;
    }
    /**
     * Field name
     */
    private String name;
    public TapField name(String name) {
        this.name = name;
        return this;
    }
    /**
     * Primary key
     */
    private Boolean isPrimaryKey;
    public TapField isPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        return this;
    }
    /**
     * Partition key
     */
    private Boolean isPartitionKey;
    public TapField isPartitionKey(Boolean isPartitionKey) {
        this.isPartitionKey = isPartitionKey;
        return this;
    }
    /**
     * Partition key position, start from 1.
     */
    private Integer partitionKeyPos;
    public TapField partitionKeyPos(Integer partitionKeyPos) {
        this.partitionKeyPos = partitionKeyPos;
        return this;
    }
    /**
     * Field position, start from 1.
     */
    private Integer pos;
    public TapField pos(Integer pos) {
        this.pos = pos;
        return this;
    }
    /**
     * Primary key position, start from 1.
     */
    private Integer primaryKeyPos;
    public TapField primaryKeyPos(Integer primaryKeyPos) {
        this.primaryKeyPos = primaryKeyPos;
        return this;
    }
    /**
     * Foreign key table name.
     */
    private String foreignKeyTable;
    public TapField foreignKeyTable(String foreignKeyTable) {
        this.foreignKeyTable = foreignKeyTable;
        return this;
    }
    /**
     * Foreign key field name.
     */
    private String foreignKeyField;
    public TapField foreignKeyField(String foreignKeyField) {
        this.foreignKeyField = foreignKeyField;
        return this;
    }
    /**
     * Field default value.
     */
    private Object defaultValue;
    public TapField defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
    /**
     * Auto incremental
     */
    private Boolean autoInc;
    public TapField autoInc(Boolean autoInc) {
        this.autoInc = autoInc;
        return this;
    }
    /**
     * Increment start value
     */
    private Long autoIncStartValue;
    public TapField autoIncStartValue(Long autoIncStartValue) {
        this.autoIncStartValue = autoIncStartValue;
        return this;
    }
//    /**
//     * Unique field
//     */
//    private Boolean unique;
//    public TapField unique(Boolean unique) {
//        this.unique = unique;
//        return this;
//    }
    /**
     * Check expression, ensure the data can only be write when satisfy the check expression.
     */
    private String check;
    public TapField check(String check) {
        this.check = check;
        return this;
    }
    /**
     * Field comment
     */
    private String comment;
    public TapField comment(String comment) {
        this.comment = comment;
        return this;
    }

    /**
     *
     */
    private String constraint;
    public TapField constraint(String constraint) {
        this.constraint = constraint;
        return this;
    }
    /**
     * Standard types
     * TapType, TapString, TapNumber, TapArray, etc
     */
    private TapType tapType;
    public TapField tapType(TapType tapType) {
        this.tapType = tapType;
        return this;
    }

    public TapField clone() {
        TapField newField = new TapField();
        newField.nullable = nullable;
        newField.name = name;
        newField.originType = originType;
        newField.partitionKeyPos = partitionKeyPos;
        newField.pos = pos;
        newField.primaryKeyPos = primaryKeyPos;
        newField.autoInc = autoInc;
        newField.autoIncStartValue = autoIncStartValue;
        newField.check = check;
        newField.comment = comment;
        newField.constraint = constraint;
        newField.defaultValue = defaultValue;
        newField.foreignKeyField = foreignKeyField;
        newField.foreignKeyTable = foreignKeyTable;
        newField.isPartitionKey = isPartitionKey;
        newField.isPrimaryKey = isPrimaryKey;
        newField.tapType = tapType; //XXX need clone?
        return newField;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public TapType getTapType() {
        return tapType;
    }

    public void setTapType(TapType tapType) {
        this.tapType = tapType;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public Integer getPartitionKeyPos() {
        return partitionKeyPos;
    }

    public void setPartitionKeyPos(Integer partitionKeyPos) {
        this.partitionKeyPos = partitionKeyPos;
    }

    public Integer getPos() {
        return pos;
    }

    public void setPos(Integer pos) {
        this.pos = pos;
    }

    public Integer getPrimaryKeyPos() {
        return primaryKeyPos;
    }

    public void setPrimaryKeyPos(Integer primaryKeyPos) {
        this.primaryKeyPos = primaryKeyPos;
    }

    public String getForeignKeyTable() {
        return foreignKeyTable;
    }

    public void setForeignKeyTable(String foreignKeyTable) {
        this.foreignKeyTable = foreignKeyTable;
    }

    public String getForeignKeyField() {
        return foreignKeyField;
    }

    public void setForeignKeyField(String foreignKeyField) {
        this.foreignKeyField = foreignKeyField;
    }

    public Boolean getAutoInc() {
        return autoInc;
    }

    public void setAutoInc(Boolean autoInc) {
        this.autoInc = autoInc;
    }

    public Long getAutoIncStartValue() {
        return autoIncStartValue;
    }

    public void setAutoIncStartValue(Long autoIncStartValue) {
        this.autoIncStartValue = autoIncStartValue;
    }

//    public Boolean getUnique() {
//        return unique;
//    }
//
//    public void setUnique(Boolean unique) {
//        this.unique = unique;
//    }

    public String getCheck() {
        return check;
    }

    public void setCheck(String check) {
        this.check = check;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

    public Boolean getPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(Boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public Boolean getPartitionKey() {
        return isPartitionKey;
    }

    public void setPartitionKey(Boolean partitionKey) {
        isPartitionKey = partitionKey;
    }

    public String getOriginType() {
        return originType;
    }

    public void setOriginType(String originType) {
        this.originType = originType;
    }
}
