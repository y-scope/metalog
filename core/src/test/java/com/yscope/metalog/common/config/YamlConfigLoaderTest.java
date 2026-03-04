package com.yscope.metalog.common.config;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.coordinator.workflow.consolidation.policy.PolicyConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for YamlConfigLoader hot-reload infrastructure.
 *
 * <p>Validates: - YAML parsing - Thread-safe config access - Hot-reload via file watching - State
 * transfer callbacks
 */
class YamlConfigLoaderTest {

  @TempDir Path tempDir;

  private Path configFile;
  private YamlConfigLoader<PolicyConfig> config;

  @BeforeEach
  void setUp() throws IOException {
    configFile = tempDir.resolve("policy.yaml");

    // Write initial config
    String initialYaml =
        "policies:\n"
            + "  - name: test-policy\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 4\n"
            + "    archiveSize:\n"
            + "      targetMb: 64\n";
    Files.writeString(configFile, initialYaml);
  }

  @AfterEach
  void tearDown() {
    if (config != null) {
      config.close();
    }
  }

  // Parsing tests

  @Test
  void testLoad_fromFile_success() {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    PolicyConfig loaded = config.get();
    assertNotNull(loaded);
    assertEquals(1, loaded.getPolicies().size());
    assertEquals("test-policy", loaded.getPolicies().get(0).getName());
  }

  @Test
  void testLoad_fromClasspath_success() {
    // Load from classpath resource (policy.yaml)
    config = new YamlConfigLoader<>(PolicyConfig.class, "config/policy.yaml");

    PolicyConfig loaded = config.get();
    assertNotNull(loaded);
    assertFalse(loaded.getPolicies().isEmpty());
  }

  @Test
  void testLoad_fileTakesPrecedence() throws IOException {
    // File should take precedence over classpath
    String fileYaml =
        "policies:\n" + "  - name: file-policy\n" + "    type: spark\n" + "    enabled: true\n";
    Files.writeString(configFile, fileYaml);

    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, "config/policy.yaml");

    PolicyConfig loaded = config.get();
    assertEquals("file-policy", loaded.getPolicies().get(0).getName());
  }

  @Test
  void testLoad_invalidPath_fallsBackToClasspath() {
    Path nonExistent = tempDir.resolve("nonexistent.yaml");

    config = new YamlConfigLoader<>(PolicyConfig.class, nonExistent, "config/policy.yaml");

    // Should load from classpath
    assertNotNull(config.get());
  }

  @Test
  void testLoad_neitherExists_throws() {
    Path nonExistent = tempDir.resolve("nonexistent.yaml");

    assertThrows(
        YamlConfigLoader.ConfigException.class,
        () -> {
          new YamlConfigLoader<>(PolicyConfig.class, nonExistent, "also-nonexistent.yaml");
        });
  }

  // Thread safety tests

  @Test
  void testGet_threadSafe() throws InterruptedException {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    // Access from multiple threads
    CountDownLatch latch = new CountDownLatch(10);
    AtomicReference<Throwable> error = new AtomicReference<>();

    for (int i = 0; i < 10; i++) {
      new Thread(
              () -> {
                try {
                  for (int j = 0; j < 100; j++) {
                    PolicyConfig c = config.get();
                    assertNotNull(c);
                  }
                } catch (Throwable t) {
                  error.set(t);
                } finally {
                  latch.countDown();
                }
              })
          .start();
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertNull(error.get());
  }

  // Hot-reload tests

  @Test
  void testReloadConfig_updatesConfig() throws IOException {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    String oldName = config.get().getPolicies().get(0).getName();
    assertEquals("test-policy", oldName);

    // Write new config
    String newYaml =
        "policies:\n" + "  - name: updated-policy\n" + "    type: spark\n" + "    enabled: true\n";
    Files.writeString(configFile, newYaml);

    // Manually trigger reload
    config.reloadConfig();

    String newName = config.get().getPolicies().get(0).getName();
    assertEquals("updated-policy", newName);
  }

  @Test
  void testReloadConfig_invalidYaml_retainsPrevious() throws IOException {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    String oldName = config.get().getPolicies().get(0).getName();

    // Write invalid YAML
    Files.writeString(configFile, "invalid: yaml: content: [[[");

    // Reload should fail but retain previous config
    config.reloadConfig();

    // Previous config should be retained
    assertEquals(oldName, config.get().getPolicies().get(0).getName());
  }

  @Test
  void testStartWatching_callbackInvoked() throws IOException, InterruptedException {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    CountDownLatch reloadLatch = new CountDownLatch(1);
    AtomicReference<PolicyConfig> oldConfigRef = new AtomicReference<>();
    AtomicReference<PolicyConfig> newConfigRef = new AtomicReference<>();

    config.startWatching(
        (oldConfig, newConfig) -> {
          oldConfigRef.set(oldConfig);
          newConfigRef.set(newConfig);
          reloadLatch.countDown();
        });

    // Give the watcher time to start
    Thread.sleep(100);

    // Write new config
    String newYaml =
        "policies:\n" + "  - name: watched-update\n" + "    type: spark\n" + "    enabled: true\n";
    Files.writeString(configFile, newYaml);

    // Wait for callback (may take time due to file system events)
    boolean reloaded = reloadLatch.await(5, TimeUnit.SECONDS);

    assertTrue(reloaded, "File watcher callback was not triggered within timeout");
    assertNotNull(oldConfigRef.get());
    assertNotNull(newConfigRef.get());
    assertEquals("watched-update", newConfigRef.get().getPolicies().get(0).getName());
  }

  @Test
  void testStopWatching_stopsWatcher() {
    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    config.startWatching(null);
    assertTrue(config.isWatching());

    config.stopWatching();
    assertFalse(config.isWatching());
  }

  @Test
  void testStartWatching_noPath_doesNothing() {
    config = new YamlConfigLoader<>(PolicyConfig.class, "config/policy.yaml");

    config.startWatching(null);

    // Should not crash, but also shouldn't be watching
    assertFalse(config.isWatching());
  }

  // Complex config tests

  @Test
  void testLoad_policyConfig_allFields() throws IOException {
    String complexYaml =
        "policies:\n"
            + "  - name: complex-policy\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    groupingKey: dim/str128/application_id\n"
            + "    triggers:\n"
            + "      jobComplete:\n"
            + "        minFiles: 5\n"
            + "      timeout:\n"
            + "        hours: 8\n"
            + "    archiveSize:\n"
            + "      targetMb: 128\n"
            + "      maxMb: 256\n"
            + "    compressionRatio:\n"
            + "      initial: 4.0\n"
            + "      learningRate: 0.2\n";
    Files.writeString(configFile, complexYaml);

    config = new YamlConfigLoader<>(PolicyConfig.class, configFile, null);

    PolicyConfig loaded = config.get();
    PolicyConfig.PolicyDefinition policy = loaded.getPolicies().get(0);

    assertEquals("complex-policy", policy.getName());
    assertEquals("spark", policy.getType());
    assertTrue(policy.isEnabled());
    assertEquals("dim/str128/application_id", policy.getGroupingKey());
    assertEquals(5, policy.getTriggers().getJobComplete().getMinFiles());
    assertEquals(8, policy.getTriggers().getTimeout().getHours());
    assertEquals(128, policy.getArchiveSize().getTargetMb());
    assertEquals(256, policy.getArchiveSize().getMaxMb());
    assertEquals(4.0, policy.getCompressionRatio().getInitialRatio(), 0.01);
    assertEquals(0.2, policy.getCompressionRatio().getLearningRate(), 0.01);
  }

}
