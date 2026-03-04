package com.yscope.metalog.coordinator.ingestion;

import com.yscope.metalog.coordinator.grpc.proto.AggEntry;
import com.yscope.metalog.coordinator.grpc.proto.AggType;
import com.yscope.metalog.coordinator.grpc.proto.ArchiveFileInfo;
import com.yscope.metalog.coordinator.grpc.proto.DimEntry;
import com.yscope.metalog.coordinator.grpc.proto.DimensionValue;
import com.yscope.metalog.coordinator.grpc.proto.FileFields;
import com.yscope.metalog.coordinator.grpc.proto.IrFileInfo;
import com.yscope.metalog.coordinator.grpc.proto.MetadataRecord;
import com.yscope.metalog.coordinator.grpc.proto.SelfDescribingEntry;
import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless converter between protobuf {@link MetadataRecord} and domain {@link FileRecord}.
 *
 * <p>Structured {@link DimEntry} values are stored in {@link FileRecord#getDimensions()} using a
 * NUL-separated composite key ({@code fieldName\0baseType\0width}, e.g. {@code
 * application_id\0str\064} for str-64) that {@code BatchingWriter} parses directly without regex.
 *
 * <p>Structured {@link AggEntry} values are stored in {@link FileRecord#getCounts()} or {@link
 * FileRecord#getFloatCounts()} using a NUL-separated composite key ({@code
 * AGGTYPE\0field\0qualifier}) that {@code BatchingWriter} recognises and maps directly to a
 * registry {@code AggFieldSpec} without pattern matching.
 *
 * <p>{@link SelfDescribingEntry} keys use the {@code /}-separated convention described in the proto
 * comments and are parsed into the same composite key format.
 */
public final class ProtoConverter {
  private static final Logger logger = LoggerFactory.getLogger(ProtoConverter.class);

  private ProtoConverter() {}

  /**
   * Convert a protobuf {@link MetadataRecord} to a domain {@link FileRecord}.
   *
   * @param record the protobuf record
   * @return the domain FileRecord
   * @throws IllegalArgumentException if the state string is invalid
   */
  public static FileRecord toFileRecord(MetadataRecord record) {
    FileRecord file = new FileRecord();

    FileFields ff = record.getFile();

    // State
    file.setState(FileState.valueOf(ff.getState()));

    // Timestamps
    file.setMinTimestamp(ff.getMinTimestamp());
    file.setMaxTimestamp(ff.getMaxTimestamp());

    // Metrics
    file.setRecordCount(ff.getRecordCount());
    file.setRawSizeBytes(zeroToNull(ff.getRawSizeBytes()));

    // Retention
    file.setRetentionDays(ff.getRetentionDays());
    file.setExpiresAt(ff.getExpiresAt());

    // IR file info (absent = archive-only entry)
    if (ff.hasIr()) {
      IrFileInfo ir = ff.getIr();
      file.setIrStorageBackend(ir.getClpIrStorageBackend());
      file.setIrBucket(ir.getClpIrBucket());
      file.setIrPath(ir.getClpIrPath());
      file.setIrSizeBytes(zeroToNull(ir.getClpIrSizeBytes()));
    }

    // Archive file info (absent = not yet consolidated)
    if (ff.hasArchive()) {
      ArchiveFileInfo ar = ff.getArchive();
      file.setArchiveStorageBackend(ar.getClpArchiveStorageBackend());
      file.setArchiveBucket(ar.getClpArchiveBucket());
      file.setArchivePath(ar.getClpArchivePath());
      file.setArchiveCreatedAt(ar.getClpArchiveCreatedAt());
      file.setArchiveSizeBytes(zeroToNull(ar.getClpArchiveSizeBytes()));
    }

    // Structured dimensions: store using composite key (fieldName\0baseType\0width)
    for (DimEntry dim : record.getDimList()) {
      String compositeKey = buildDimCompositeKey(dim);
      Object value = extractDimValue(dim.getValue());
      if (compositeKey != null && value != null) {
        file.setDimension(compositeKey, value);
      }
    }

    // Structured aggregations: store using composite key (AGGTYPE\0field\0qualifier[\0aliasCol])
    for (AggEntry agg : record.getAggList()) {
      AggregationType aggType = toAggregationType(agg.getAggType());
      String compositeKey = aggType.name() + "\0" + agg.getField() + "\0" + agg.getQualifier();
      if (!agg.getAliasColumn().isEmpty()) {
        compositeKey += "\0" + agg.getAliasColumn();
      }
      switch (agg.getValueCase()) {
        case INT_VAL -> file.setIntAgg(compositeKey, agg.getIntVal());
        case FLOAT_VAL -> file.setFloatAgg(compositeKey, agg.getFloatVal());
        default -> logger.warn("AggEntry for field '{}' has no value set", agg.getField());
      }
    }

    // Self-describing KV: parse "/" key convention
    for (SelfDescribingEntry entry : record.getSelfDescribingKvList()) {
      String key = entry.getKey();
      String rawValue = entry.getValue();
      if (key.startsWith("dim/")) {
        Object value = parseDimValue(key, rawValue);
        file.setDimension(key, value);
      } else if (key.startsWith("agg_int/") || key.startsWith("agg_float/")) {
        parseSelfDescAgg(key, rawValue, file);
      } else {
        logger.warn("Ignoring self-describing entry '{}': unknown prefix", key);
      }
    }

    return file;
  }

  /**
   * Build a composite dimension key from a {@link DimEntry}.
   *
   * <p>The key format is {@code fieldName\0baseType\0width} (e.g. {@code application_id\0str\064}
   * for str-64), which {@code BatchingWriter} can parse directly without regex.
   */
  static String buildDimCompositeKey(DimEntry dim) {
    String fieldName = dim.getKey();
    DimensionValue val = dim.getValue();
    return switch (val.getValueCase()) {
      case STR -> {
        int w = val.getStr().getMaxLength() > 0 ? val.getStr().getMaxLength() : 255;
        yield fieldName + "\0str\0" + w;
      }
      case STR_UTF8 -> {
        int w = val.getStrUtf8().getMaxLength() > 0 ? val.getStrUtf8().getMaxLength() : 255;
        yield fieldName + "\0str_utf8\0" + w;
      }
      case INT_VAL -> fieldName + "\0int\0" + 0;
      case BOOL_VAL -> fieldName + "\0bool\0" + 0;
      case FLOAT_VAL -> fieldName + "\0float\0" + 0;
      default -> null;
    };
  }

  private static Object extractDimValue(DimensionValue val) {
    return switch (val.getValueCase()) {
      case STR -> val.getStr().getValue();
      case STR_UTF8 -> val.getStrUtf8().getValue();
      case INT_VAL -> val.getIntVal();
      case BOOL_VAL -> val.getBoolVal();
      case FLOAT_VAL -> val.getFloatVal();
      default -> null;
    };
  }

  /**
   * Parse a self-describing agg key (e.g. {@code agg_int/gte/level/debug}) and store the value in
   * the appropriate FileRecord map using the composite key format.
   *
   * <p>Key format: {@code agg_int/<agg_type>/<field>} (3 parts, no qualifier) or {@code
   * agg_int/<agg_type>/<field>/<qualifier>} (4 parts, with qualifier).
   */
  static void parseSelfDescAgg(String key, String rawValue, FileRecord file) {
    boolean isFloat = key.startsWith("agg_float/");
    String[] parts = key.split("/", 4);
    if (parts.length < 3) {
      logger.warn("Ignoring self-describing agg '{}': key must have at least 3 parts", key);
      return;
    }

    String typePart = parts[1];
    String field = parts[2];
    String qualifier = parts.length == 4 ? parts[3] : "";

    AggregationType aggType;
    try {
      aggType = AggregationType.valueOf(typePart.toUpperCase());
    } catch (IllegalArgumentException e) {
      logger.warn("Ignoring self-describing agg '{}': unknown agg_type '{}'", key, typePart);
      return;
    }

    String compositeKey = aggType.name() + "\0" + field + "\0" + qualifier;
    try {
      if (isFloat) {
        file.setFloatAgg(compositeKey, Double.parseDouble(rawValue));
      } else {
        file.setIntAgg(compositeKey, Long.parseLong(rawValue));
      }
    } catch (NumberFormatException e) {
      logger.warn("Ignoring self-describing agg '{}': invalid numeric value '{}'", key, rawValue);
    }
  }

  /**
   * Parse a self-describing dimension value string into the appropriate Java type based on the key.
   *
   * <p>Key format: {@code dim/<typeSpec>/<field>} where typeSpec is one of {@code str{N}}, {@code
   * str{N}utf8}, {@code int}, {@code bool}, {@code float}.
   *
   * @param key the dimension key (e.g., {@code dim/str128/service})
   * @param rawValue the string representation of the value
   * @return the parsed value (String, Long, Double, or Boolean)
   */
  static Object parseDimValue(String key, String rawValue) {
    String[] parts = key.split("/", 3);
    if (parts.length < 2) {
      return rawValue;
    }
    String typeSpec = parts[1];
    if (typeSpec.equals("int")) {
      return Long.parseLong(rawValue);
    } else if (typeSpec.equals("float")) {
      return Double.parseDouble(rawValue);
    } else if (typeSpec.equals("bool")) {
      return Boolean.parseBoolean(rawValue);
    }
    return rawValue; // str variants (str{N}, str{N}utf8)
  }

  private static AggregationType toAggregationType(AggType protoType) {
    return switch (protoType) {
      case GTE -> AggregationType.GTE;
      case GT -> AggregationType.GT;
      case LTE -> AggregationType.LTE;
      case LT -> AggregationType.LT;
      case SUM -> AggregationType.SUM;
      case AVG -> AggregationType.AVG;
      case MIN -> AggregationType.MIN;
      case MAX -> AggregationType.MAX;
      default -> AggregationType.EQ;
    };
  }

  private static Long zeroToNull(long value) {
    return value == 0 ? null : value;
  }
}
