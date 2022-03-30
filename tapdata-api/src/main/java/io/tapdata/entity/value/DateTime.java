package io.tapdata.entity.value;

import java.util.Objects;
import java.util.TimeZone;

public class DateTime {
    /**
     * 秒数
     */
    private Long seconds;
    /**
     * 纳秒
     *
     * 毫秒， 微秒， 纳秒， 1000
     */
    private Long nano;
    /**
     * 时区 GMT+8
     */
    private TimeZone timeZone;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTime that = (DateTime) o;
        return Objects.equals(seconds, that.seconds) &&
                Objects.equals(timeZone, that.timeZone) &&
                Objects.equals(nano, that.nano);
    }

    public Long getSeconds() {
        return seconds;
    }

    public void setSeconds(Long seconds) {
        this.seconds = seconds;
    }

    public Long getNano() {
        return nano;
    }

    public void setNano(Long nano) {
        this.nano = nano;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
