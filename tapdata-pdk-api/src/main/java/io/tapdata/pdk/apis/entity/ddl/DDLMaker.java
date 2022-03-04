package io.tapdata.pdk.apis.entity.ddl;


/**
 * @author dxtr
 */
public interface DDLMaker<T> {
  default T createTable() {
    throw new UnsupportedOperationException();
  }
}
