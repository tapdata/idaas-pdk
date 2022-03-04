package io.tapdata.pdk.apis.functions.common;

import io.tapdata.pdk.apis.valuemapping.TapValueClassProvider;

/**
 * @author Dexter
 */
public interface ValueClassFunction {
  default TapValueClassProvider provideTapValueClass() {
    return new TapValueClassProvider() {};
  };
}
