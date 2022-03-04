package io.tapdata.pdk.apis.valuemapping;


/**
 * The Abstract class for Tap Value container, all Tap Value should extend this.
 *
 * @author Dexter
 */
public abstract class AbstractTapValue<T> {
  public static final String GET_ORIGIN = "getOrigin";
  public static final String GET_STRING = "getString";

  private T converted;
  private Object origin;
  private ITapValueConverter<T> converter;

  public Object getOrigin() {
    return origin;
  }

  public void setOrigin(Object origin) {
    this.origin = origin;
  }

  public void setConverter(ITapValueConverter<T> converter) {
    this.converter = converter;
  }

  /**
   * Get the converted type {@code T}
   *
   * @return T
   */
  public T get() throws Exception {
    if (converter == null) {
      throw new Exception("");
    }
    if (converted == null) {
      converted = converter.convert();
    }
    return converted;
  }

  // Getters: All the getters should accept an AbstractTapValue container as parameter
  // and convert it to the desired database acceptable values.

  /**
   * Get the origin value from Tap Value container.
   */
  public Object getOrigin(AbstractTapValue<?> container) throws Exception {
    return container.getOrigin();
  }

  /**
   * Get the String value from Tap Value container.
   */
  public String getString(AbstractTapValue<?> container) throws Exception {
    return container.get().toString();
  }
}
