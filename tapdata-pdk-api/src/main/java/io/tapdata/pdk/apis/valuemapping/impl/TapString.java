package io.tapdata.pdk.apis.valuemapping.impl;

import io.tapdata.pdk.apis.valuemapping.AbstractTapValue;

/**
 * @author Dexter
 */
public class TapString extends AbstractTapValue<String> {
  public static String GET_STRING = "getString";
  public static String GET_BOOLEAN = "getBoolean";

  // Constructors

  public TapString() {
  }

  /**
   * Accept a {@code String} value into TapString.
   *
   * @param origin: {@code String} Value to be accepted into TapString.
   */
  public TapString(String origin) {
    this.setOrigin(origin);
    this.setConverter(() -> origin);
  }

  // Getters: Getting desired value from container
  public Boolean getBoolean(AbstractTapValue<String> value) throws Exception {
    if (value == null) {
      return false;
    }
    String origin = value.get();
//    if (StringUtils.isBlank(origin)) {
    if (origin == null) {
      return false;
    }
    if (origin.equalsIgnoreCase("y") || origin.equalsIgnoreCase("1")) {
      return true;
    } else {
      return false;
    }
  }
}
