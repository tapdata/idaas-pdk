package io.tapdata.pdk.apis.annotations;

import java.lang.annotation.*;

@Target(value = {ElementType.TYPE})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface TapConnector {
  /**
   * config path
   * @return
   */
  String value();
}
