package com.yscope.metalog.common.storage;

import com.yscope.metalog.common.config.ObjectStorageConfig;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Factory for constructing {@link StorageBackend} instances and {@link StorageRegistry} from
 * configuration.
 *
 * <p>Backends are auto-discovered via classpath scanning. To add a custom storage backend:
 *
 * <ol>
 *   <li>Implement {@link StorageBackend} (or extend {@link ObjectStorageBackend})
 *   <li>Annotate with {@code @StorageBackendType("your-type")}
 *   <li>Done — no other changes needed!
 * </ol>
 *
 * <p>Additional packages can be scanned by setting the {@code CLP_STORAGE_BACKEND_PACKAGES}
 * environment variable to a comma-separated list of package names.
 *
 * <p>For object storage, builds a shared {@link S3AsyncClient} per endpoint and creates per-bucket
 * {@link ObjectStorageBackend} instances that share the client.
 */
public class StorageBackendFactory {
  private static final Logger logger = LoggerFactory.getLogger(StorageBackendFactory.class);

  /** Default package to scan for storage backends (includes subpackages). */
  private static final String DEFAULT_STORAGE_PACKAGE = "com.yscope.metalog.common.storage";

  /** Environment variable for additional storage backend packages. */
  private static final String STORAGE_PACKAGES_ENV = "CLP_STORAGE_BACKEND_PACKAGES";

  /** Registry of backend type name -> class. */
  private static final Map<String, Class<? extends StorageBackend>> REGISTRY =
      new ConcurrentHashMap<>();

  static {
    List<String> packagesToScan = new ArrayList<>();
    packagesToScan.add(DEFAULT_STORAGE_PACKAGE);

    String extraPackages = System.getenv(STORAGE_PACKAGES_ENV);
    if (extraPackages != null && !extraPackages.isBlank()) {
      for (String pkg : extraPackages.split(",")) {
        String trimmed = pkg.trim();
        if (!trimmed.isEmpty()) {
          packagesToScan.add(trimmed);
        }
      }
    }

    logger.info("Scanning for storage backends in packages: {}", packagesToScan);

    try (ScanResult scanResult =
        new ClassGraph()
            .acceptPackages(packagesToScan.toArray(new String[0]))
            .enableClassInfo()
            .enableAnnotationInfo()
            .scan()) {

      for (ClassInfo classInfo :
          scanResult.getClassesWithAnnotation(StorageBackendType.class)) {
        if (classInfo.implementsInterface(StorageBackend.class)) {
          try {
            @SuppressWarnings("unchecked")
            Class<? extends StorageBackend> clazz =
                (Class<? extends StorageBackend>) classInfo.loadClass();

            StorageBackendType annotation = clazz.getAnnotation(StorageBackendType.class);
            String typeName = annotation.value();

            REGISTRY.put(typeName, clazz);
            logger.debug(
                "Registered storage backend: {} -> {}", typeName, clazz.getSimpleName());

          } catch (Exception e) {
            logger.error("Failed to load storage backend class: {}", classInfo.getName(), e);
          }
        }
      }
    } catch (Exception e) {
      logger.error(
          "ClassGraph scanning failed for storage backend discovery (unsupported platform?). "
              + "Backend creation will fall back to an empty registry.",
          e);
    }

    logger.info(
        "StorageBackendFactory initialized with {} backends: {}",
        REGISTRY.size(),
        REGISTRY.keySet());
  }

  private StorageBackendFactory() {}

  /**
   * Build a complete StorageRegistry from generic backend configurations.
   *
   * <p>Supports any backend type (object storage, HTTP, filesystem, etc.) Each backend config must
   * include a {@code "type"} field indicating the backend type.
   *
   * <p>Example backend config:
   *
   * <pre>{@code
   * backends:
   *   tb:
   *     type: tb
   *     baseUrl: http://localhost:19617
   *   minio:
   *     type: minio
   *     endpoint: http://localhost:9000
   *     accessKey: minioadmin
   *     secretKey: minioadmin
   * }</pre>
   *
   * @param defaultBackend name of the default backend
   * @param backends map of backend name to configuration (must include "type" field)
   * @return fully constructed StorageRegistry
   */
  public static StorageRegistry createRegistry(
      String defaultBackend, Map<String, Map<String, Object>> backends) {

    if (backends == null || backends.isEmpty()) {
      throw new IllegalArgumentException("At least one backend must be configured");
    }

    Map<String, Object> defaultConfig = backends.get(defaultBackend);
    if (defaultConfig == null) {
      throw new IllegalArgumentException(
          "Default backend '" + defaultBackend + "' not found in backends: " + backends.keySet());
    }

    // Create backends based on type
    Map<String, StorageBackend> allBackends = new LinkedHashMap<>();
    for (var entry : backends.entrySet()) {
      String name = entry.getKey();
      Map<String, Object> config = entry.getValue();

      // Determine backend type from config (defaults to backend name if not specified)
      String type = config.getOrDefault("type", name).toString();

      StorageBackend backend = create(name, type, config);
      allBackends.put(name, backend);
      logger.debug("Created backend: name={}, type={}", name, type);
    }

    // Get IR and archive backends (must be the default backend)
    StorageBackend irBackend = allBackends.get(defaultBackend);
    StorageBackend archiveBackend = irBackend; // Same backend for both by default

    logger.info(
        "Created StorageRegistry: defaultBackend={}, backends={}",
        defaultBackend, allBackends.keySet());

    return new StorageRegistry(irBackend, archiveBackend, allBackends);
  }

  /**
   * Build a complete StorageRegistry from Node/Worker storage configuration.
   *
   * <p>For object storage backends, a shared S3AsyncClient is created per backend name. The IR and
   * archive backends share this client but are bound to their respective buckets.
   *
   * @param defaultBackend name of the default backend
   * @param irBucket IR bucket name
   * @param archiveBucket archive bucket name
   * @param backends map of backend name to object storage configuration
   * @return fully constructed StorageRegistry
   */
  public static StorageRegistry createObjectStorageRegistry(
      String defaultBackend,
      String irBucket,
      String archiveBucket,
      Map<String, ObjectStorageConfig> backends) {

    if (backends == null || backends.isEmpty()) {
      throw new IllegalArgumentException("At least one backend must be configured");
    }

    ObjectStorageConfig defaultConfig = backends.get(defaultBackend);
    if (defaultConfig == null) {
      throw new IllegalArgumentException(
          "Default backend '" + defaultBackend + "' not found in backends: " + backends.keySet());
    }

    // Build one shared async client for the default backend
    S3AsyncClient sharedClient = ObjectStorageBackend.buildClient(defaultConfig);

    // Create per-bucket backends sharing the same client
    ObjectStorageBackend irBackend =
        createObjectBackend(defaultBackend, sharedClient, irBucket, false);
    ObjectStorageBackend archiveBackend;
    if (irBucket.equals(archiveBucket)) {
      archiveBackend = irBackend;
    } else {
      archiveBackend = createObjectBackend(defaultBackend, sharedClient, archiveBucket, false);
    }

    // Build all-backends map; the IR and archive backends are registered under
    // composite names (backendName + ":" + bucket) for per-record routing.
    // Also add clients for non-default backends.
    Map<String, StorageBackend> allBackends = new LinkedHashMap<>();
    allBackends.put(defaultBackend + ":" + irBucket, irBackend);
    if (!irBucket.equals(archiveBucket)) {
      allBackends.put(defaultBackend + ":" + archiveBucket, archiveBackend);
    }

    // Create clients for additional backends
    for (var entry : backends.entrySet()) {
      if (!entry.getKey().equals(defaultBackend)) {
        S3AsyncClient otherClient = ObjectStorageBackend.buildClient(entry.getValue());
        // Register with the IR bucket by default; additional buckets can be added later
        ObjectStorageBackend otherBackend =
            createObjectBackend(entry.getKey(), otherClient, irBucket, true);
        allBackends.put(entry.getKey() + ":" + irBucket, otherBackend);
      }
    }

    // Also register backends by their plain name (for backward compatibility routing)
    allBackends.put(defaultBackend, irBackend);

    logger.info(
        "Created StorageRegistry: defaultBackend={}, irBucket={}, archiveBucket={}, backends={}",
        defaultBackend, irBucket, archiveBucket, allBackends.keySet());

    // The sharedClient is owned by the backends — close will be handled by the last backend
    // that uses it. We mark the archive backend as the client owner if it's separate.
    return new StorageRegistry(irBackend, archiveBackend, allBackends) {
      private final S3AsyncClient ownedClient = sharedClient;

      @Override
      public void close() {
        super.close();
        try {
          ownedClient.close();
        } catch (Exception e) {
          logger.warn("Error closing shared S3 client", e);
        }
      }
    };
  }

  /**
   * Create a single StorageBackend from a type string and configuration.
   *
   * <p>Looks up the backend class from the registry and calls its {@code (String, Map)} constructor
   * via reflection.
   *
   * @param name backend name
   * @param type backend type (e.g., "local", "http")
   * @param config configuration map
   * @return constructed StorageBackend
   */
  public static StorageBackend create(String name, String type, Map<String, Object> config) {
    Class<? extends StorageBackend> clazz = REGISTRY.get(type);
    if (clazz == null) {
      throw new IllegalArgumentException(
          "Unknown backend type: '" + type + "' for " + name + ". Available: " + REGISTRY.keySet());
    }

    try {
      Constructor<? extends StorageBackend> ctor =
          clazz.getConstructor(String.class, Map.class);
      return ctor.newInstance(name, config);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Backend type '" + type + "' (" + clazz.getSimpleName()
              + ") does not have a (String, Map) constructor",
          e);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to instantiate backend: " + clazz.getName(), e);
    }
  }

  /**
   * Create a typed ObjectStorageBackend subclass based on the backend name.
   *
   * <p>Looks up the class from the registry and verifies it extends {@link ObjectStorageBackend}.
   * Falls back to the base {@code ObjectStorageBackend} for unrecognized names.
   *
   * @param name backend name ("minio", "s3", or "gcs")
   * @param client shared S3 async client
   * @param bucket bucket name
   * @param ownsClient if true, closing the backend closes the client
   * @return typed ObjectStorageBackend instance
   */
  static ObjectStorageBackend createObjectBackend(
      String name, S3AsyncClient client, String bucket, boolean ownsClient) {
    Class<? extends StorageBackend> clazz = REGISTRY.get(name);

    if (clazz != null && ObjectStorageBackend.class.isAssignableFrom(clazz)) {
      try {
        Constructor<? extends StorageBackend> ctor =
            clazz.getConstructor(S3AsyncClient.class, String.class, boolean.class);
        return (ObjectStorageBackend) ctor.newInstance(client, bucket, ownsClient);
      } catch (Exception e) {
        logger.warn(
            "Failed to instantiate {} via reflection, falling back to base ObjectStorageBackend",
            clazz.getSimpleName(),
            e);
      }
    }

    // Fall back to base ObjectStorageBackend for unrecognized names
    return new ObjectStorageBackend(name, client, bucket, ownsClient);
  }

  /**
   * Look up the backend class for a given type name.
   *
   * @param type backend type name (case-sensitive)
   * @return the registered class, or {@code null} if unknown
   */
  public static Class<? extends StorageBackend> getBackendClass(String type) {
    return REGISTRY.get(type);
  }

  /** Get all registered storage backend type names. */
  public static Set<String> getRegisteredTypes() {
    return Collections.unmodifiableSet(REGISTRY.keySet());
  }
}
