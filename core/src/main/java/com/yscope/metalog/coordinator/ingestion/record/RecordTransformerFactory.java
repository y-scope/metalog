package com.yscope.metalog.coordinator.ingestion.record;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Name-based lookup for {@link RecordTransformer} implementations.
 *
 * <p>Transformers are auto-discovered via classpath scanning. To add a custom transformer:
 *
 * <ol>
 *   <li>Implement {@link RecordTransformer}
 *   <li>Annotate with {@code @TransformerName("your-name")}
 *   <li>Done - no other changes needed!
 * </ol>
 *
 * <p>Then set {@code record_transformer = 'your-name'} in {@code _table_config}.
 *
 * <p>Additional packages can be scanned by setting the {@code CLP_TRANSFORMER_PACKAGES} environment
 * variable to a comma-separated list of package names.
 */
public class RecordTransformerFactory {

  private static final Logger logger = LoggerFactory.getLogger(RecordTransformerFactory.class);

  /** Default package to scan for transformers (includes subpackages) */
  private static final String DEFAULT_TRANSFORMER_PACKAGE =
      "com.yscope.metalog.coordinator.ingestion.record";

  /** Environment variable for additional transformer packages */
  private static final String TRANSFORMER_PACKAGES_ENV = "CLP_TRANSFORMER_PACKAGES";

  /** Registry of transformer name -> supplier (lazy instantiation) */
  private static final Map<String, Supplier<RecordTransformer>> REGISTRY =
      new ConcurrentHashMap<>();

  static {
    // Build list of packages to scan
    List<String> packagesToScan = new ArrayList<>();
    packagesToScan.add(DEFAULT_TRANSFORMER_PACKAGE);

    // Add additional packages from environment variable
    String extraPackages = System.getenv(TRANSFORMER_PACKAGES_ENV);
    if (extraPackages != null && !extraPackages.isBlank()) {
      for (String pkg : extraPackages.split(",")) {
        String trimmed = pkg.trim();
        if (!trimmed.isEmpty()) {
          packagesToScan.add(trimmed);
        }
      }
    }

    logger.info("Scanning for transformers in packages: {}", packagesToScan);

    // Auto-discover transformers via classpath scanning
    try (ScanResult scanResult =
        new ClassGraph()
            .acceptPackages(packagesToScan.toArray(new String[0]))
            .enableClassInfo()
            .enableAnnotationInfo()
            .scan()) {

      for (ClassInfo classInfo : scanResult.getClassesWithAnnotation(TransformerName.class)) {
        if (classInfo.implementsInterface(RecordTransformer.class)) {
          try {
            @SuppressWarnings("unchecked")
            Class<? extends RecordTransformer> clazz =
                (Class<? extends RecordTransformer>) classInfo.loadClass();

            TransformerName annotation = clazz.getAnnotation(TransformerName.class);
            String name = annotation.value().toLowerCase();

            REGISTRY.put(name, () -> newInstance(clazz));
            logger.debug("Registered transformer: {} -> {}", name, clazz.getSimpleName());

          } catch (Exception e) {
            logger.error("Failed to load transformer class: {}", classInfo.getName(), e);
          }
        }
      }
    } catch (Exception e) {
      logger.error(
          "ClassGraph scanning failed for transformer discovery (unsupported platform?). "
              + "Transformer creation will fall back to an empty registry.",
          e);
    }

    logger.info(
        "RecordTransformerFactory initialized with {} transformers: {}",
        REGISTRY.size(),
        REGISTRY.keySet());
  }

  /**
   * Create a {@link RecordTransformer} by name.
   *
   * @param name transformer name from {@code _table_config.record_transformer}; null or empty or
   *     {@code "default"} returns {@link DefaultRecordTransformer}
   * @return the transformer instance
   * @throws IllegalArgumentException if the name is not recognized
   */
  public static RecordTransformer create(String name) {
    if (name == null || name.isEmpty()) {
      name = "default";
    }

    String key = name.toLowerCase();
    Supplier<RecordTransformer> supplier = REGISTRY.get(key);

    if (supplier == null) {
      throw new IllegalArgumentException(
          "Unknown record transformer: '" + name + "'. Available: " + REGISTRY.keySet());
    }

    return supplier.get();
  }

  /** Get all registered transformer names. */
  public static Set<String> getRegisteredNames() {
    return Collections.unmodifiableSet(REGISTRY.keySet());
  }

  private static RecordTransformer newInstance(Class<? extends RecordTransformer> clazz) {
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate transformer: " + clazz.getName(), e);
    }
  }
}
