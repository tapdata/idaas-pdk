package io.tapdata.pdk.apis.valuemapping;

import io.tapdata.pdk.apis.valuemapping.impl.*;

/**
 * @author dxtr
 */
public interface TapValueClassProvider {
  /**
   * Get the Tap Value container class for Tap Type {@code Boolean}. The default container
   * for {@code Boolean} values is {@link TapBoolean}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Boolean}, you should
   * overwrite this function and return the extended class. You can find more information on how
   * to extend the container  at {@link TapBoolean}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Boolean}.
   */
  default Class<? extends TapBoolean> getTapBooleanClass() {
    return TapBoolean.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Bytes}. The default container for
   * {@code Bytes} values is {@link TapBytes}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Bytes}, you should
   * overwrite this function and return the extended class. You can find more information on
   * how to extend the container at {@link TapBytes}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Bytes}.
   */
  default Class<? extends TapBytes> getTapBytesClass() {
    return TapBytes.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Date}. The default container for
   * {@code Date} values is {@link TapDate}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Date}, you should
   * overwrite this function and return the extended class. You can find more information on
   * how to extend the container at {@link TapDate}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Date}.
   */
  default Class<? extends TapDate> getTapDateClass() {
    return TapDate.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Datetime}. The default container
   * for {@code Datetime} values is {@link TapDatetime}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Datetime}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapDatetime}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Datetime}.
   */
  default Class<? extends TapDatetime> getTapDatetimeClass() {
    return TapDatetime.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Number}. The default container
   * for {@code Number} values is {@link TapNumber}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Number}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapNumber}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Number}.
   */
  default Class<? extends TapNumber> getTapNumberClass() {
    return TapNumber.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code String}. The default container
   * for {@code String} values is {@link TapString}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code String}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapString}. </p>
   *
   * @return Tap Value container class for Tap Type {@code String}.
   */
  default Class<? extends TapString> getTapStringClass() {
    return TapString.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Time}. The default container
   * for {@code Time} values is {@link TapTime}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Time}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapTime}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Time}.
   */
  default Class<? extends TapTime> getTapTimeClass() {
    return TapTime.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Array}. The default container
   * for {@code Array} values is {@link TapArray}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Array}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapArray}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Array}.
   */
  default Class<? extends TapArray> getTapArrayClass() {
    return TapArray.class;
  }

  /**
   * Get the Tap Value container class for Tap Type {@code Map}. The default container
   * for {@code Map} values is {@link TapMap}.
   *
   * <p> If you want to extend the Tap Value container for Tap Type {@code Map}, you
   * should overwrite this function and return the extended class. You can find more
   * information on how to extend the container at {@link TapMap}. </p>
   *
   * @return Tap Value container class for Tap Type {@code Map}.
   */
  default Class<? extends TapMap> getTapMapClass() {
    return TapMap.class;
  }
}
