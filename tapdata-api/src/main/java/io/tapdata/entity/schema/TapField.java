package io.tapdata.entity.schema;


import io.tapdata.entity.type.TapType;

public class TapField {
    public TapField() {}
    public TapField(String name, TapType tapType) {
        this.name = name;
        this.tapType = tapType;
    }

    private String originType;
    /**
     * 可以为空
     */
    private Boolean nullable;
    /**
     * 字段名字
     */
    private String name;
    /**
     * 主键
     */
    private Boolean isPrimaryKey;
    /**
     * 分区主键
     */
    private Boolean isPartitionKey;
    /**
     * 分区主键的位置
     */
    private Long partitionKeyPos;
    /**
     * 字段位置
     */
    private Long pos;
    /**
     * 主键位置
     */
    private Long primaryKeyPos;
    /**
     * 外键表名
     */
    private String foreignKeyTable;
    /**
     * 外键字段
     */
    private String foreignKeyField;
    /**
     * 默认值
     */
    private Object defaultValue;
    /**
     * 是否自增
     */
    private Boolean autoInc;
    /**
     * 自增默认值， 自增起始值
     */
    private Long autoIncStartValue;
    /**
     * 唯一主键
     */
    private Boolean unique;
    /**
     * 检查表达式， 例如a > 9这样的检查， 不满足条件就写不进去
     */
    private String check;
    /**
     * 字段注释
     */
    private String comment;
    /**
     * 暂时不管
     */
    private String constraint;
    /**
     * 字段类型
     */
    private TapType tapType;

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
    public Long getPartitionKeyPos() {
        return partitionKeyPos;
    }

    public void setPartitionKeyPos(Long partitionKeyPos) {
        this.partitionKeyPos = partitionKeyPos;
    }

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public Long getPrimaryKeyPos() {
        return primaryKeyPos;
    }

    public void setPrimaryKeyPos(Long primaryKeyPos) {
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

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

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
