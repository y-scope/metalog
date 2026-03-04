package com.yscope.metalog.coordinator.ingestion.record.transformers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yscope.metalog.coordinator.grpc.proto.MetadataRecord;
import com.yscope.metalog.coordinator.ingestion.ProtoConverter;
import com.yscope.metalog.coordinator.ingestion.record.RecordTransformer;
import com.yscope.metalog.coordinator.ingestion.record.TransformerName;
import com.yscope.metalog.metastore.model.FileRecord;
import java.io.IOException;

/**
 * Default transformer that auto-detects payload format per message.
 *
 * <p>JSON payloads start with {@code '{'} (0x7B) and are deserialized directly into {@link FileRecord}.
 * All other payloads are treated as serialized {@link MetadataRecord} protobuf and converted via
 * {@link ProtoConverter}.
 */
@TransformerName("default")
public class DefaultRecordTransformer implements RecordTransformer {

  @Override
  public FileRecord transform(byte[] payload, ObjectMapper objectMapper)
      throws RecordTransformException {
    if (payload.length > 0 && payload[0] == '{') {
      try {
        return objectMapper.readValue(payload, FileRecord.class);
      } catch (IOException e) {
        throw new RecordTransformException("Failed to parse JSON payload as FileRecord", e);
      }
    }
    try {
      return ProtoConverter.toFileRecord(MetadataRecord.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      throw new RecordTransformException("Failed to parse payload as MetadataRecord proto", e);
    }
  }
}
