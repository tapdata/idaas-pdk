package io.tapdata.pdk.apis.valuemapping.impl;

import io.tapdata.pdk.apis.valuemapping.AbstractTapValue;

/**
 *
 * @author Dexter
 */
public class TapBoolean extends AbstractTapValue<Boolean> {
  public static String GET_BOOLEAN = "getBoolean";

  // Constructors

  public TapBoolean() {}

  /**
   * Accept a {@link Boolean} value into TapBoolean.
   */
  public TapBoolean(Boolean origin) {
    this.setOrigin(origin);
    this.setConverter(() -> origin);
  }

  public TapBoolean(boolean origin) {
    this((Boolean) origin);
  }

  // Getters: Getting desired value from container

  /**
   * Convert TapBoolean container to a {@code boolean}.
   */
  public boolean getBoolean(AbstractTapValue<?> container) throws Exception {
    return ((TapBoolean)container).get();
  }
}
