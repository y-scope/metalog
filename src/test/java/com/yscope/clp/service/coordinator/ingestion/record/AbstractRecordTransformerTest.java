package com.yscope.clp.service.coordinator.ingestion.record;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yscope.clp.service.metastore.model.FileRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AbstractRecordTransformer")
class AbstractRecordTransformerTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private TestTransformer transformer;

  /** Concrete subclass to expose protected methods for testing. */
  static class TestTransformer extends AbstractRecordTransformer {
    @Override
    public FileRecord transform(byte[] payload, ObjectMapper objectMapper)
        throws RecordTransformer.RecordTransformException {
      return new FileRecord();
    }
  }

  @BeforeEach
  void setUp() {
    transformer = new TestTransformer();
  }

  // ==========================================
  // String Dimension
  // ==========================================

  @Nested
  @DisplayName("setStringDimension")
  class SetStringDimension {

    @Test
    void setsFromLegacyField() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("service_name", "web-api");

      transformer.setStringDimension(file, root, "service_name", 128);

      assertEquals("web-api", file.getDimension("dim/str128/service_name"));
    }

    @Test
    void prefersSlashDelimitedFormat() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("dim/str128/service_name", "new-format");
      root.put("service_name", "legacy-format");

      transformer.setStringDimension(file, root, "service_name", 128);

      assertEquals("new-format", file.getDimension("dim/str128/service_name"));
    }

    @Test
    void skipsEmptyValue() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("service_name", "");

      transformer.setStringDimension(file, root, "service_name", 128);

      assertNull(file.getDimension("dim/str128/service_name"));
    }

    @Test
    void skipsUnknownValue() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("service_name", "unknown");

      transformer.setStringDimension(file, root, "service_name", 128);

      assertNull(file.getDimension("dim/str128/service_name"));
    }

    @Test
    void skipsUnknownCaseInsensitive() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("service_name", "UNKNOWN");

      transformer.setStringDimension(file, root, "service_name", 128);

      assertNull(file.getDimension("dim/str128/service_name"));
    }
  }

  // ==========================================
  // String Dimension UTF-8
  // ==========================================

  @Nested
  @DisplayName("setStringDimensionUtf8")
  class SetStringDimensionUtf8 {

    @Test
    void setsWithUtf8KeyFormat() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("username", "こんにちは");

      transformer.setStringDimensionUtf8(file, root, "username", 256);

      assertEquals("こんにちは", file.getDimension("dim/str256utf8/username"));
    }

    @Test
    void prefersSlashDelimitedFormat() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("dim/str256utf8/username", "new-format");
      root.put("username", "legacy-format");

      transformer.setStringDimensionUtf8(file, root, "username", 256);

      assertEquals("new-format", file.getDimension("dim/str256utf8/username"));
    }
  }

  // ==========================================
  // Int Dimension
  // ==========================================

  @Nested
  @DisplayName("setIntDimension")
  class SetIntDimension {

    @Test
    void setsFromLegacyField() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("priority", 5);

      transformer.setIntDimension(file, root, "priority");

      assertEquals(5L, file.getDimension("dim/int/priority"));
    }

    @Test
    void skipsNonNumericValue() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("priority", "not-a-number");

      transformer.setIntDimension(file, root, "priority");

      assertNull(file.getDimension("dim/int/priority"));
    }
  }

  // ==========================================
  // Bool Dimension
  // ==========================================

  @Nested
  @DisplayName("setBoolDimension")
  class SetBoolDimension {

    @Test
    void setsTrue() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("is_error", true);

      transformer.setBoolDimension(file, root, "is_error");

      assertEquals(true, file.getDimension("dim/bool/is_error"));
    }

    @Test
    void setsFalse() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("is_error", false);

      transformer.setBoolDimension(file, root, "is_error");

      assertEquals(false, file.getDimension("dim/bool/is_error"));
    }
  }

  // ==========================================
  // Float Dimension
  // ==========================================

  @Nested
  @DisplayName("setFloatDimension")
  class SetFloatDimension {

    @Test
    void setsFromLegacyField() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("cpu_usage", 3.14);

      transformer.setFloatDimension(file, root, "cpu_usage");

      assertEquals(3.14, (Double) file.getDimension("dim/float/cpu_usage"), 0.001);
    }

    @Test
    void skipsNonNumericValue() {
      FileRecord file = new FileRecord();
      ObjectNode root = mapper.createObjectNode();
      root.put("cpu_usage", "not-a-number");

      transformer.setFloatDimension(file, root, "cpu_usage");

      assertNull(file.getDimension("dim/float/cpu_usage"));
    }
  }

  // ==========================================
  // Integer Aggregation
  // ==========================================

  @Nested
  @DisplayName("setIntAgg")
  class SetIntAgg {

    @Test
    void withQualifier() {
      FileRecord file = new FileRecord();
      transformer.setIntAgg(file, "gte", "level", "warn", 42L);

      assertEquals(42L, file.getIntAgg("agg_int/gte/level/warn"));
    }

    @Test
    void withoutQualifier() {
      FileRecord file = new FileRecord();
      transformer.setIntAgg(file, "eq", "record_count", null, 100L);

      assertEquals(100L, file.getIntAgg("agg_int/eq/record_count"));
    }

    @Test
    void emptyQualifier() {
      FileRecord file = new FileRecord();
      transformer.setIntAgg(file, "eq", "record_count", "", 100L);

      assertEquals(100L, file.getIntAgg("agg_int/eq/record_count"));
    }
  }

  // ==========================================
  // Float Aggregation
  // ==========================================

  @Nested
  @DisplayName("setFloatAgg")
  class SetFloatAgg {

    @Test
    void withQualifier() {
      FileRecord file = new FileRecord();
      transformer.setFloatAgg(file, "sum", "cpu", "usage", 3.14);

      assertEquals(3.14, file.getFloatAgg("agg_float/sum/cpu/usage"), 0.001);
    }

    @Test
    void withoutQualifier() {
      FileRecord file = new FileRecord();
      transformer.setFloatAgg(file, "avg", "latency", null, 99.5);

      assertEquals(99.5, file.getFloatAgg("agg_float/avg/latency"), 0.001);
    }
  }

  // ==========================================
  // Field Extraction
  // ==========================================

  @Nested
  @DisplayName("Field extraction helpers")
  class FieldExtraction {

    @Test
    void getRequiredString_present() throws RecordTransformer.RecordTransformException {
      ObjectNode root = mapper.createObjectNode();
      root.put("name", "test-value");

      assertEquals("test-value", transformer.getRequiredString(root, "name"));
    }

    @Test
    void getRequiredString_missingThrows() {
      ObjectNode root = mapper.createObjectNode();
      assertThrows(
          RecordTransformer.RecordTransformException.class,
          () -> transformer.getRequiredString(root, "name"));
    }

    @Test
    void getRequiredLong_present() throws RecordTransformer.RecordTransformException {
      ObjectNode root = mapper.createObjectNode();
      root.put("count", 42L);

      assertEquals(42L, transformer.getRequiredLong(root, "count"));
    }

    @Test
    void getRequiredLong_missingThrows() {
      ObjectNode root = mapper.createObjectNode();
      assertThrows(
          RecordTransformer.RecordTransformException.class,
          () -> transformer.getRequiredLong(root, "count"));
    }

    @Test
    void getOptionalString_present() {
      ObjectNode root = mapper.createObjectNode();
      root.put("name", "value");

      assertEquals("value", transformer.getOptionalString(root, "name", "default"));
    }

    @Test
    void getOptionalString_missingReturnsDefault() {
      ObjectNode root = mapper.createObjectNode();
      assertEquals("default", transformer.getOptionalString(root, "name", "default"));
    }

    @Test
    void getOptionalLong_present() {
      ObjectNode root = mapper.createObjectNode();
      root.put("count", 42L);

      assertEquals(42L, transformer.getOptionalLong(root, "count", 0L));
    }

    @Test
    void getOptionalLong_missingReturnsDefault() {
      ObjectNode root = mapper.createObjectNode();
      assertEquals(0L, transformer.getOptionalLong(root, "count", 0L));
    }
  }

  // ==========================================
  // Key Normalization
  // ==========================================

  @Nested
  @DisplayName("normalizeKeys")
  class NormalizeKeys {

    @Test
    void lowercasesAllKeys() throws Exception {
      ObjectNode root = mapper.createObjectNode();
      root.put("ServiceName", "web");
      root.put("LEVEL", "error");
      root.put("already_lower", "ok");

      ObjectNode normalized = transformer.normalizeKeys(root, mapper);

      assertEquals("web", normalized.get("servicename").asText());
      assertEquals("error", normalized.get("level").asText());
      assertEquals("ok", normalized.get("already_lower").asText());
      assertNull(normalized.get("ServiceName"));
      assertNull(normalized.get("LEVEL"));
    }
  }
}
