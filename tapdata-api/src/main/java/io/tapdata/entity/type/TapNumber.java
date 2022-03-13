package io.tapdata.entity.type;

public class TapNumber extends TapType {
    /**
     * 存储位数， 字节数， 不同数据库实现不一样
     * 数字的几位数， 小数点后面几位数
     *
     * 有length是单精度， float(4), length就是4
     *
     * 双精度， float(8, 2), length没有值， precision是8， scale是2， 小数点前8位， 小数点后2位
     */
    private Long length;
    /**
     * 最小值
     */
    private Long min;
    /**
     * 最大值
     */
    private Long max;
    /**
     *
     */
    private Long precision;
    private Long scale;

    public Long getLength() {
        return length;
    }
    public void setLength(Long length) {
        this.length = length;
    }

    public Long getMin() {
        return min;
    }
    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }
    public void setMax(Long max) {
        this.max = max;
    }

    public Long getPrecision() {
        return precision;
    }
    public void setPrecision(Long precision) {
        this.precision = precision;
    }

    public Long getScale() {
        return scale;
    }
    public void setScale(Long scale) {
        this.scale = scale;
    }
}
