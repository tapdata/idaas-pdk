package io.tapdata.connector.postgres.storage;

import io.tapdata.connector.postgres.PostgresOffset;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresOffsetStorage {

    public static Map<String, PostgresOffset> postgresOffsetMap = new ConcurrentHashMap<>(); //one slot one key
    public static Map<String, List<PostgresOffset>> manyOffsetMap = new ConcurrentHashMap<>(); //one slot many offsets

}
