package io.tapdata.pdk.apis.valuemapping;

/**
 * @author Dexter
 */
public interface ITapValueConverter<T> {
  public T convert() throws Exception;
}
