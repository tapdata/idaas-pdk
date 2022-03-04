package io.tapdata.pdk.apis.valuemapping.tmp;

import java.sql.Timestamp;
import java.time.*;
import java.util.*;

/**
 * @author Dexter
 */
public class DateUtil {

  /**
   * convert timestamp milli seconds
   *
   * @param timestamp
   * @param fromTimeZone
   * @param toTimeZone
   * @return
   */
  public static long convertTimestamp(long timestamp, TimeZone fromTimeZone, TimeZone toTimeZone) {
    LocalDateTime dt = LocalDateTime.now();
    ZonedDateTime fromZonedDateTime = dt.atZone(fromTimeZone.toZoneId());
    ZonedDateTime toZonedDateTime = dt.atZone(toTimeZone.toZoneId());
    long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();

    return timestamp + diff;
  }

  public static Timestamp instant2Timestamp(Instant instant, ZoneId toZoneId) throws Exception {
    if (instant == null) {
      throw new NullPointerException();
    }

    ZonedDateTime zonedDateTime = instant.atZone(toZoneId);
    return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
  }


}

