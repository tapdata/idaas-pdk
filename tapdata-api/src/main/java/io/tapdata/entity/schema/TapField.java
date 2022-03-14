package io.tapdata.entity.schema;


import io.tapdata.entity.type.TapType;

public class TapField {
    public TapField() {}

    public TapField(String name, TapType tapType) {
        this(name, tapType, null);
    }

    public TapField(String name, TapType tapType, String originType) {
        this.name = name;
        this.tapType = tapType;
        this.originType = originType;
    }

    private String originType;
    public TapField originType(String originType) {
        this.originType = originType;
        return this;
    }
    /**
     * 可以为空
     */
    private Boolean nullable;
    public TapField nullable(Boolean nullable) {
        this.nullable = nullable;
        return this;
    }
    /**
     * 字段名字
     */
    private String name;
    public TapField name(String name) {
        this.name = name;
        return this;
    }
    /**
     * 主键
     */
    private Boolean isPrimaryKey;
    public TapField isPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        return this;
    }
    /**
     * 分区主键
     */
    private Boolean isPartitionKey;
    public TapField isPartitionKey(Boolean isPartitionKey) {
        this.isPartitionKey = isPartitionKey;
        return this;
    }
    /**
     * 分区主键的位置
     */
    private Long partitionKeyPos;
    public TapField partitionKeyPos(Long partitionKeyPos) {
        this.partitionKeyPos = partitionKeyPos;
        return this;
    }
    /**
     * 字段位置
     */
    private Long pos;
    public TapField pos(Long pos) {
        this.pos = pos;
        return this;
    }
    /**
     * 主键位置
     */
    private Long primaryKeyPos;
    public TapField primaryKeyPos(Long primaryKeyPos) {
        this.primaryKeyPos = primaryKeyPos;
        return this;
    }
    /**
     * 外键表名
     */
    private String foreignKeyTable;
    public TapField foreignKeyTable(String foreignKeyTable) {
        this.foreignKeyTable = foreignKeyTable;
        return this;
    }
    /**
     * 外键字段
     */
    private String foreignKeyField;
    public TapField foreignKeyField(String foreignKeyField) {
        this.foreignKeyField = foreignKeyField;
        return this;
    }
    /**
     * 默认值
     */
    private Object defaultValue;
    public TapField defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
    /**
     * 是否自增
     */
    private Boolean autoInc;
    public TapField autoInc(Boolean autoInc) {
        this.autoInc = autoInc;
        return this;
    }
    /**
     * 自增默认值， 自增起始值
     */
    private Long autoIncStartValue;
    public TapField autoIncStartValue(Long autoIncStartValue) {
        this.autoIncStartValue = autoIncStartValue;
        return this;
    }
    /**
     * 唯一主键
     */
    private Boolean unique;
    public TapField unique(Boolean unique) {
        this.unique = unique;
        return this;
    }
    /**
     * 检查表达式， 例如a > 9这样的检查， 不满足条件就写不进去
     */
    private String check;
    public TapField check(String check) {
        this.check = check;
        return this;
    }
    /**
     * 字段注释
     */
    private String comment;
    public TapField comment(String comment) {
        this.comment = comment;
        return this;
    }

    /**
     * 暂时不管
     */
    private String constraint;
    public TapField constraint(String constraint) {
        this.constraint = constraint;
        return this;
    }
    /**
     * 字段类型
     */
    private TapType tapType;
    public TapField tapType(TapType tapType) {
        this.tapType = tapType;
        return this;
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
