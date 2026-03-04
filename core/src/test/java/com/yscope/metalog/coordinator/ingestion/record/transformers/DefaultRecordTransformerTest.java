package com.yscope.metalog.coordinator.ingestion.record.transformers;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.yscope.metalog.coordinator.ingestion.record.RecordTransformer;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DefaultRecordTransformer")
class DefaultRecordTransformerTest {

  private DefaultRecordTransformer transformer;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    transformer = new DefaultRecordTransformer();
    objectMapper = new ObjectMapper();
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
  }

  // ==========================================
  // JSON Payload
  // ==========================================

  @Nested
  @DisplayName("JSON payload")
  class JsonPayload {

    @Test
    void validJson_parsesSuccessfully() throws RecordTransformer.RecordTransformException {
      String json =
          "{\"ir_path\":\"test/path.clp\","
              + "\"state\":\"IR_ARCHIVE_BUFFERING\","
              + "\"min_timestamp\":1000,"
              + "\"max_timestamp\":2000,"
              + "\"record_count\":42}";
      byte[] payload = json.getBytes();

      FileRecord result = transformer.transform(payload, objectMapper);

      assertNotNull(result);
      assertEquals("test/path.clp", result.getIrPath());
      assertEquals(FileState.IR_ARCHIVE_BUFFERING, result.getState());
      assertEquals(1000L, result.getMinTimestamp());
      assertEquals(2000L, result.getMaxTimestamp());
      assertEquals(42L, result.getRecordCount());
    }

    @Test
    void invalidJson_throws() {
      byte[] payload = "{invalid json}".getBytes();
      assertThrows(
          RecordTransformer.RecordTransformException.class,
          () -> transformer.transform(payload, objectMapper));
    }
  }

  // ==========================================
  // Protobuf Payload
  // ==========================================

  @Nested
  @DisplayName("Protobuf payload")
  class ProtobufPayload {

    @Test
    void invalidProtobuf_throws() {
      byte[] payload = new byte[] {0x01, 0x02, 0x03, 0x04};
      assertThrows(
          RecordTransformer.RecordTransformException.class,
          () -> transformer.transform(payload, objectMapper));
    }
  }

  // ==========================================
  // Edge Cases
  // ==========================================

  @Nested
  @DisplayName("Edge cases")
  class EdgeCases {

    @Test
    void emptyPayload_throws() {
      // Empty byte array doesn't start with '{', so treated as protobuf.
      // Empty protobuf has default empty-string state which is not a valid FileState enum.
      byte[] payload = new byte[0];
      assertThrows(
          IllegalArgumentException.class,
          () -> transformer.transform(payload, objectMapper));
    }
  }
}
