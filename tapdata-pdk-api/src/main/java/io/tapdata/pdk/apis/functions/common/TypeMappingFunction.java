package io.tapdata.pdk.apis.functions.common;

import io.tapdata.pdk.apis.typemapping.TypeMappingProvider;

/**
 * @author Dexter
 */
public interface TypeMappingFunction {
  TypeMappingProvider provideTypeMapping();
}
