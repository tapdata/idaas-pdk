package io.tapdata.entity.schema.value;

import java.time.Instant;
import java.util.Date;
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
    //nano int
    private Integer nano;
    /**
     * 时区 GMT+8
     */
    //ZoneId
    private TimeZone timeZone;

    public DateTime() {

    }

    public DateTime(Instant instant) {
        seconds = instant.getEpochSecond();
        nano = instant.getNano();
    }

    public DateTime(Date date) {

    }

    public Date toDate() {
        Long milliseconds;
        if(seconds != null) {
            milliseconds = seconds * 1000;
            if(nano != null) {
                milliseconds += milliseconds + (nano / 1000 / 1000);
            }
        } else {
            return null;
        }
        return new Date(milliseconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTime that = (DateTime) o;
        return Objects.equals(seconds, that.seconds) &&
                Objects.equals(timeZone, that.timeZone) &&
                Objects.equals(nano, that.nano);
    }

    @Override
    public String toString() {
        return "DateTime nano " + nano + " seconds " + seconds + " timeZone " + timeZone;
    }

    public Long getSeconds() {
        return seconds;
    }

    public void setSeconds(Long seconds) {
        this.seconds = seconds;
    }

    public Integer getNano() {
        return nano;
    }

    public void setNano(Integer nano) {
        this.nano = nano;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
