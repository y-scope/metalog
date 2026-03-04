package com.yscope.clp.service.common.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic YAML configuration loader with hot-reload support.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Thread-safe config access via AtomicReference
 *   <li>Hot-reload using WatchService
 *   <li>Optional callback for state transfer on reload
 *   <li>Fallback to classpath resources
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>
 * YamlConfigLoader&lt;PolicyConfig&gt; config = new YamlConfigLoader&lt;&gt;(
 *     PolicyConfig.class,
 *     Paths.get("/etc/clp/policy.yaml"),
 *     "config/policy.yaml"  // classpath fallback
 * );
 * config.startWatching((oldConfig, newConfig) -&gt; {
 *     // Transfer state from old to new
 * });
 * PolicyConfig current = config.get();
 * </pre>
 *
 * @param <T> The configuration type
 */
public class YamlConfigLoader<T> implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(YamlConfigLoader.class);

  /** Pattern for environment variable placeholders: ${VAR_NAME:default} or ${VAR_NAME} */
  private static final Pattern PLACEHOLDER_PATTERN =
      Pattern.compile("\\$\\{([^}:]+)(?::([^}]+))?\\}");

  private final Class<T> configClass;
  private final Path configPath;
  private final String classpathFallback;
  private final ObjectMapper mapper;
  private final AtomicReference<T> configRef;

  private volatile WatchService watchService;
  private volatile Thread watchThread;
  private final AtomicBoolean watching = new AtomicBoolean(false);
  private volatile BiConsumer<T, T> reloadCallback;

  /**
   * Create a YAML config loader.
   *
   * @param configClass The configuration class to deserialize into
   * @param configPath Path to the external YAML file (may be null)
   * @param classpathFallback Classpath resource name for fallback (may be null)
   * @throws ConfigException if neither path nor fallback exists
   */
  public YamlConfigLoader(Class<T> configClass, Path configPath, String classpathFallback) {
    this.configClass = configClass;
    this.configPath = configPath;
    this.classpathFallback = classpathFallback;
    this.mapper = createMapper();
    this.configRef = new AtomicReference<>();

    // Load initial configuration
    T initial = loadConfig();
    if (initial == null) {
      throw new ConfigException(
          "Failed to load configuration from path="
              + configPath
              + " or classpath="
              + classpathFallback);
    }
    configRef.set(initial);
    logger.info("Loaded {} configuration", configClass.getSimpleName());
  }

  /**
   * Create a YAML config loader with only classpath fallback.
   *
   * @param configClass The configuration class
   * @param classpathFallback Classpath resource name
   */
  public YamlConfigLoader(Class<T> configClass, String classpathFallback) {
    this(configClass, null, classpathFallback);
  }

  private ObjectMapper createMapper() {
    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return om;
  }

  /**
   * Get the current configuration.
   *
   * @return The current configuration (never null after construction)
   */
  public T get() {
    return configRef.get();
  }

  /**
   * Start watching for configuration file changes.
   *
   * <p>When the config file changes, it is reloaded and the optional callback is invoked with
   * (oldConfig, newConfig) to allow state transfer.
   *
   * @param callback Optional callback invoked on reload (may be null)
   */
  public void startWatching(BiConsumer<T, T> callback) {
    if (configPath == null || !Files.exists(configPath)) {
      logger.warn("Cannot watch for changes: config path not set or does not exist");
      return;
    }

    if (!watching.compareAndSet(false, true)) {
      logger.warn("Already watching for configuration changes");
      return;
    }

    this.reloadCallback = callback;

    try {
      watchService = FileSystems.getDefault().newWatchService();
      Path parent = configPath.getParent();
      if (parent == null) {
        parent = Paths.get(".");
      }
      parent.register(
          watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);
      watchThread =
          new Thread(this::watchLoop, "YamlConfigLoaderWatcher-" + configClass.getSimpleName());
      watchThread.setDaemon(true);
      watchThread.start();

      logger.info("Started watching for changes to {}", configPath);
    } catch (IOException e) {
      logger.error("Failed to start config file watcher", e);
    }
  }

  /** Stop watching for configuration file changes. */
  public void stopWatching() {
    watching.set(false);
    if (watchService != null) {
      try {
        watchService.close();
      } catch (IOException e) {
        logger.warn("Error closing watch service", e);
      }
    }
    if (watchThread != null) {
      watchThread.interrupt();
      try {
        watchThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    logger.info("Stopped watching for configuration changes");
  }

  private void watchLoop() {
    logger.debug("Config watcher thread started");
    while (watching.get()) {
      try {
        WatchKey key = watchService.take();

        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();

          if (kind == StandardWatchEventKinds.OVERFLOW) {
            continue;
          }

          @SuppressWarnings("unchecked")
          WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
          Path changed = pathEvent.context();

          // Check if the changed file is our config file
          if (configPath != null && configPath.getFileName().equals(changed)) {
            logger.info("Detected change to {}", configPath);
            reloadConfig();
          }
        }

        boolean valid = key.reset();
        if (!valid) {
          logger.warn("Watch key is no longer valid, stopping watcher");
          break;
        }
      } catch (InterruptedException e) {
        logger.debug("Config watcher interrupted");
        Thread.currentThread().interrupt();
        break;
      } catch (ClosedWatchServiceException e) {
        logger.debug("Watch service closed");
        break;
      } catch (RuntimeException e) {
        logger.error("Error in config watcher", e);
      }
    }
    logger.debug("Config watcher thread stopped");
  }

  /**
   * Reload the configuration from disk.
   *
   * <p>If parsing fails, the old configuration is retained.
   */
  public void reloadConfig() {
    T oldConfig = configRef.get();
    T newConfig = loadConfig();

    if (newConfig == null) {
      logger.error("Failed to reload configuration, retaining previous config");
      return;
    }

    // Atomically swap the configuration
    configRef.set(newConfig);
    logger.info("Reloaded {} configuration", configClass.getSimpleName());

    // Invoke callback for state transfer
    if (reloadCallback != null) {
      try {
        reloadCallback.accept(oldConfig, newConfig);
      } catch (RuntimeException e) {
        logger.error("Error in reload callback", e);
      }
    }
  }

  private T loadConfig() {
    // Try external file first
    if (configPath != null && Files.exists(configPath)) {
      try {
        logger.debug("Loading configuration from {}", configPath);
        String yamlContent = Files.readString(configPath, StandardCharsets.UTF_8);
        String resolved = resolvePlaceholders(yamlContent);
        return mapper.readValue(resolved, configClass);
      } catch (IOException e) {
        logger.error("Failed to parse config file: {}", configPath, e);
      }
    }

    // Fallback to classpath resource
    if (classpathFallback != null) {
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(classpathFallback)) {
        if (is != null) {
          logger.debug("Loading configuration from classpath: {}", classpathFallback);
          String yamlContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
          String resolved = resolvePlaceholders(yamlContent);
          return mapper.readValue(resolved, configClass);
        } else {
          logger.warn("Classpath resource not found: {}", classpathFallback);
        }
      } catch (IOException e) {
        logger.error("Failed to parse classpath config: {}", classpathFallback, e);
      }
    }

    return null;
  }

  /**
   * Resolve environment variable placeholders in YAML content.
   *
   * <p>Supports two formats:
   *
   * <ul>
   *   <li>${VAR_NAME} - replaced with env var value or empty string if not set
   *   <li>${VAR_NAME:default} - replaced with env var value or default if not set
   * </ul>
   *
   * @param yaml The YAML content with placeholders
   * @return YAML content with placeholders resolved
   */
  private String resolvePlaceholders(String yaml) {
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(yaml);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String varName = matcher.group(1);
      String defaultValue = matcher.group(2);
      String envValue = System.getenv(varName);

      String replacement;
      if (envValue != null) {
        replacement = envValue;
        logger.trace("Resolved placeholder ${{{}}}: {} (from env)", varName, envValue);
      } else if (defaultValue != null) {
        replacement = defaultValue;
        logger.trace("Resolved placeholder ${{{}}}: {} (default)", varName, defaultValue);
      } else {
        replacement = "";
        logger.trace("Resolved placeholder ${{{}}}: (empty, no default)", varName);
      }

      // Escape special regex characters in replacement
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  @Override
  public void close() {
    stopWatching();
  }

  /**
   * Check if the config file watcher is active.
   *
   * @return true if watching for file changes
   */
  public boolean isWatching() {
    return watching.get();
  }

  /**
   * Get the path being watched.
   *
   * @return The config file path (may be null)
   */
  public Path getConfigPath() {
    return configPath;
  }

  /** Exception thrown when configuration loading fails. */
  public static class ConfigException extends RuntimeException {
    public ConfigException(String message) {
      super(message);
    }

    public ConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
