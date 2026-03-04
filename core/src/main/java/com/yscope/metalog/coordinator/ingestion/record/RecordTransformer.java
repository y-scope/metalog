package com.yscope.metalog.coordinator.ingestion.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yscope.metalog.metastore.model.FileRecord;

/**
 * Transforms raw Kafka message payloads (JSON bytes or protobuf) into {@link FileRecord} objects.
 *
 * <p>Per-table transformers allow different wire formats to be handled transparently. Each managed
 * table can specify its transformer via the {@code record_transformer} column in {@code
 * _table_config}.
 */
public interface RecordTransformer {

  /**
   * Convert a raw Kafka message payload into an {@link FileRecord}.
   *
   * @param payload the raw Kafka record value bytes
   * @param objectMapper shared ObjectMapper (SNAKE_CASE configured) — transformers must stay
   *     stateless
   * @return parsed FileRecord
   * @throws RecordTransformException if the message cannot be converted
   */
  FileRecord transform(byte[] payload, ObjectMapper objectMapper) throws RecordTransformException;

  /** Checked exception wrapping any failure during record transformation. */
  class RecordTransformException extends Exception {
    public RecordTransformException(String message) {
      super(message);
    }

    public RecordTransformException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
