package io.tapdata.pdk.apis.valuemapping.impl;

import io.tapdata.pdk.apis.valuemapping.AbstractTapValue;
import io.tapdata.pdk.apis.valuemapping.tmp.DateUtil;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * This Tap Value type mainly contains three values:
 * <ul>
 *   <li>Year</li>
 *   <li>Month</li>
 *   <li>Day</li>
 * </ul>
 * <p> The converted value is the number of milliseconds since January 1, 1970,
 * 00:00:00 GMT. </p>
 *
 * @author Dexter
 */
public class TapDate extends AbstractTapValue<Long> {
  public static String GET_JAVA_SQL_DATE = "getJavaSqlDate";
  public static String GET_JAVA_SQL_TIMESTAMP = "getJavaSqlTimestamp";
  public static String GET_TIMESTAMP_MILLIS = "getTimestampMillis";

  // Constructors

  public TapDate() {
  }

  /**
   * Accept a {@link java.util.Date} value into TapDate.
   */
  public TapDate(java.util.Date origin) {
    this.setOrigin(origin);
    this.setConverter(origin::getTime);
  }

  /**
   * Accept a {@link Date} value into TapDate.
   */
  public TapDate(Date origin) {
    this.setOrigin(origin);
    this.setConverter(origin::getTime);
  }

  /**
   * Accept a {@link Calendar} value into TapDate.
   */
  public TapDate(Calendar origin) {
    this.setOrigin(origin);
    this.setConverter(origin::getTimeInMillis);
  }

  /**
   * Accept a {@link Timestamp} value into TapDate.
   *
   * <p> This constructor is provided since JDBC use {@link Timestamp} to present
   * DATE column data. </p>
   */
  public TapDate(Timestamp origin) {
    this.setOrigin(origin);
    this.setConverter(origin::getTime);
  }

  // Getters: Getting desired value from this

  /**
   * Get the {@link Timestamp} from the {@code TapDate} Tap Value container.
   *
   * <p> This getter is provided since JDBC use {@link Timestamp} to present
   * date related column data. </p>
   */
  public Timestamp getJavaSqlTimestamp(AbstractTapValue<?> container) throws Exception {
    if (container.getOrigin() instanceof Timestamp) {
      return (Timestamp) container.getOrigin();
    }

    Long o = (Long) container.get();
    long convertTimestamp = DateUtil.convertTimestamp(o, TimeZone.getDefault(), TimeZone.getTimeZone("GMT"));
    return new Timestamp(convertTimestamp);
  }

  /**
   * Get the {@link Date} from the {@code TapDate} Tap Value container.
   *
   * <p> This getter is provided since JDBC use {@link Date} to present
   * date related column data. </p>
   */
  public Date getJavaSqlDate(AbstractTapValue<?> container) throws Exception {
    if (container.getOrigin() instanceof Date) {
      return (Date) container.getOrigin();
    }

    return new Date(((TapDate) container).get());
  }

  /**
   * Get milliseconds since the epoch from the {@code TapDate} Tap Value container.
   */
  public Long getTimestampMillis(AbstractTapValue<?> container) throws Exception {
    return ((TapDate) container).get();
  }

  @Override
  public String getString(AbstractTapValue<?> container) throws Exception {
    return new Date(((TapDate) container).get()).toString();
  }
}
