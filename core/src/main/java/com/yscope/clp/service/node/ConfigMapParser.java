package com.yscope.clp.service.node;

import java.util.Map;

/**
 * Type-safe parser for configuration maps.
 *
 * <p>Provides null-safe access to configuration values with default fallbacks. Simplifies the
 * common pattern of parsing configuration from {@code Map<String, Object>} structures (e.g., from
 * YAML parsing).
 *
 * <p>Example usage:
 *
 * <pre>
 * ConfigMapParser parser = new ConfigMapParser(configMap);
 * String host = parser.getString("host", "localhost");
 * int port = parser.getInt("port", 3306);
 * boolean enabled = parser.getBoolean("enabled", true);
 * </pre>
 */
public final class ConfigMapParser {
  private final Map<String, Object> config;

  /**
   * Create a new parser for the given configuration map.
   *
   * @param config Configuration map (may be null)
   */
  public ConfigMapParser(Map<String, Object> config) {
    this.config = config != null ? config : Map.of();
  }

  /**
   * Get the environment variable name for a camelCase config key.
   *
   * <p>Converts camelCase to UPPER_SNAKE_CASE: {@code consolidationEnabled} → {@code
   * CONSOLIDATION_ENABLED}
   *
   * @param key camelCase configuration key
   * @return environment variable value, or null if not set
   */
  private String getEnvOverride(String key) {
    String envKey = key.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
    return System.getenv(envKey);
  }

  /**
   * Get a string value from the configuration.
   *
   * <p>Priority: environment variable > YAML value > default.
   *
   * @param key Configuration key
   * @param defaultValue Default value if key is missing or not a string
   * @return Configuration value or default
   */
  public String getString(String key, String defaultValue) {
    String env = getEnvOverride(key);
    if (env != null) {
      return env;
    }
    Object v = config.get(key);
    return v instanceof String ? (String) v : defaultValue;
  }

  /**
   * Get an integer value from the configuration.
   *
   * <p>Priority: environment variable > YAML value > default.
   *
   * @param key Configuration key
   * @param defaultValue Default value if key is missing or not a number
   * @return Configuration value or default
   */
  public int getInt(String key, int defaultValue) {
    String env = getEnvOverride(key);
    if (env != null) {
      try {
        return Integer.parseInt(env);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid integer value for config key '" + key + "' (env override): " + env, e);
      }
    }
    Object v = config.get(key);
    return v instanceof Number ? ((Number) v).intValue() : defaultValue;
  }

  /**
   * Get a long value from the configuration.
   *
   * <p>Priority: environment variable > YAML value > default.
   *
   * @param key Configuration key
   * @param defaultValue Default value if key is missing or not a number
   * @return Configuration value or default
   */
  public long getLong(String key, long defaultValue) {
    String env = getEnvOverride(key);
    if (env != null) {
      try {
        return Long.parseLong(env);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid long value for config key '" + key + "' (env override): " + env, e);
      }
    }
    Object v = config.get(key);
    return v instanceof Number ? ((Number) v).longValue() : defaultValue;
  }

  /**
   * Get a boolean value from the configuration.
   *
   * <p>Priority: environment variable > YAML value > default.
   *
   * @param key Configuration key
   * @param defaultValue Default value if key is missing or not a boolean
   * @return Configuration value or default
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String env = getEnvOverride(key);
    if (env != null) {
      return Boolean.parseBoolean(env);
    }
    Object v = config.get(key);
    return v instanceof Boolean ? (Boolean) v : defaultValue;
  }

  /**
   * Get a nested map from the configuration.
   *
   * @param key Configuration key
   * @return Nested map or null if key is missing or not a map
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> getMap(String key) {
    Object v = config.get(key);
    return v instanceof Map ? (Map<String, Object>) v : null;
  }

  /**
   * Check if the configuration contains a key.
   *
   * @param key Configuration key
   * @return true if key exists
   */
  public boolean containsKey(String key) {
    return config.containsKey(key);
  }
}
