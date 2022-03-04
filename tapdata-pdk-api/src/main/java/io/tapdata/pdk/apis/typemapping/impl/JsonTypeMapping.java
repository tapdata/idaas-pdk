package io.tapdata.pdk.apis.typemapping.impl;

import io.tapdata.pdk.apis.typemapping.DbType;
import io.tapdata.pdk.apis.typemapping.TypeMappingDirection;
import io.tapdata.pdk.apis.typemapping.TypeMappingProvider;
import io.tapdata.pdk.apis.valuemapping.impl.*;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Dexter
 */
public class JsonTypeMapping implements TypeMappingProvider {
  @Override
  public List<DbType> bindString() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withGetter(TapString.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindNumber() {
    return asList(
      DbType.DbTypeBuilder.builder("Number")
        .withGetter(TapNumber.GET_BIGDECIMAL)
        .build()
    );
  }

  @Override
  public List<DbType> bindBytes() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapBytes.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindBoolean() {
    return asList(
      DbType.DbTypeBuilder.builder("Boolean")
        .withGetter(TapBoolean.GET_BOOLEAN)
        .build()
    );
  }

  @Override
  public List<DbType> bindDate() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapDate.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindDatetime() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapDatetime.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindDatetime_with_timezone() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapDatetime.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindTime() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapTime.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindTime_with_timezone() {
    return asList(
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapDatetime.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindArray() {
    return asList(
      DbType.DbTypeBuilder.builder("Array")
        .withDirection(TypeMappingDirection.TO_TAPTYPE)
        .build(),
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapArray.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindMap() {
    return asList(
      DbType.DbTypeBuilder.builder("Object")
        .withDirection(TypeMappingDirection.TO_TAPTYPE)
        .build(),
      DbType.DbTypeBuilder.builder("String")
        .withDirection(TypeMappingDirection.TO_DATATYPE)
        .withGetter(TapArray.GET_STRING)
        .build()
    );
  }

  @Override
  public List<DbType> bindNull() {
    return asList(
      DbType.DbTypeBuilder.builder("null")
        .build()
    );
  }
}
