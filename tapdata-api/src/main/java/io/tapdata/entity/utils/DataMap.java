package io.tapdata.entity.utils;


import java.util.LinkedHashMap;

/**
 *
 */
public class DataMap extends LinkedHashMap<String, Object> {
	public static DataMap create() {
		return new DataMap();
	}

	public <T> DataMap kv(String key, T value) {
		super.put(key, value);
		return this;
	}

	public <T> T getValue(String key, T defaultValue) {
		T value = (T) super.get(key);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

	public String getString(String key) {
		return String.valueOf(super.getOrDefault(key, ""));
	}
}
