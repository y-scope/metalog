package com.yscope.metalog.coordinator.ingestion.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yscope.metalog.metastore.model.FileRecord;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for record transformers providing common utility methods.
 *
 * <p>Provides helpers for:
 *
 * <ul>
 *   <li>Setting dimensions using self-describing slash-delimited format
 *   <li>Setting aggregations using self-describing slash-delimited format
 *   <li>Required/optional field extraction
 *   <li>JSON key normalization
 * </ul>
 */
public abstract class AbstractRecordTransformer implements RecordTransformer {

  // === Dimension helpers (self-describing slash-delimited format) ===

  /**
   * Set a string dimension using self-describing format: {@code dim/str{N}/{field}}
   *
   * <p>Example: {@code setStringDimension(file, root, "service_name", 128)} produces key {@code
   * dim/str128/service_name}
   *
   * <p>Supports migration from plain field names to slash-delimited format:
   *
   * <ul>
   *   <li>First checks for slash-delimited key (e.g., {@code dim/str128/service_name})
   *   <li>Falls back to plain field name (e.g., {@code service_name}) and converts
   * </ul>
   *
   * @param file The FileRecord to populate
   * @param root The JSON object
   * @param jsonField The source field name (lowercase recommended)
   * @param maxLength Maximum column width (e.g., 128 for VARCHAR(128))
   */
  protected void setStringDimension(
      FileRecord file, ObjectNode root, String jsonField, int maxLength) {
    String key = String.format("dim/str%d/%s", maxLength, jsonField);

    // Try slash-delimited format first (new format)
    if (root.has(key) && !root.get(key).isNull()) {
      String value = root.get(key).asText();
      if (!value.isEmpty() && !"unknown".equalsIgnoreCase(value)) {
        file.setDimension(key, value);
      }
      return;
    }

    // Fallback to plain field name (legacy format)
    if (root.has(jsonField) && !root.get(jsonField).isNull()) {
      String value = root.get(jsonField).asText();
      if (!value.isEmpty() && !"unknown".equalsIgnoreCase(value)) {
        file.setDimension(key, value);
      }
    }
  }

  /**
   * Set a UTF-8 string dimension using self-describing format: {@code dim/str{N}utf8/{field}}
   *
   * <p>Use for fields containing Unicode characters (e.g., user names, descriptions).
   *
   * <p>Supports migration from plain field names to slash-delimited format.
   *
   * @param file The FileRecord to populate
   * @param root The JSON object
   * @param jsonField The source field name
   * @param maxLength Maximum column width
   */
  protected void setStringDimensionUtf8(
      FileRecord file, ObjectNode root, String jsonField, int maxLength) {
    String key = String.format("dim/str%dutf8/%s", maxLength, jsonField);

    // Try slash-delimited format first (new format)
    if (root.has(key) && !root.get(key).isNull()) {
      String value = root.get(key).asText();
      if (!value.isEmpty() && !"unknown".equalsIgnoreCase(value)) {
        file.setDimension(key, value);
      }
      return;
    }

    // Fallback to plain field name (legacy format)
    if (root.has(jsonField) && !root.get(jsonField).isNull()) {
      String value = root.get(jsonField).asText();
      if (!value.isEmpty() && !"unknown".equalsIgnoreCase(value)) {
        file.setDimension(key, value);
      }
    }
  }

  /**
   * Set an integer dimension using self-describing format: {@code dim/int/{field}}
   *
   * <p>Supports migration from plain field names to slash-delimited format.
   *
   * @param file The FileRecord to populate
   * @param root The JSON object
   * @param jsonField The source field name
   */
  protected void setIntDimension(FileRecord file, ObjectNode root, String jsonField) {
    String key = String.format("dim/int/%s", jsonField);

    // Try slash-delimited format first (new format)
    if (root.has(key) && !root.get(key).isNull()) {
      JsonNode node = root.get(key);
      try {
        long value = node.isNumber() ? node.asLong() : Long.parseLong(node.asText());
        file.setDimension(key, value);
      } catch (NumberFormatException e) {
        // Skip if not a valid number
      }
      return;
    }

    // Fallback to plain field name (legacy format)
    if (root.has(jsonField) && !root.get(jsonField).isNull()) {
      JsonNode node = root.get(jsonField);
      try {
        long value = node.isNumber() ? node.asLong() : Long.parseLong(node.asText());
        file.setDimension(key, value);
      } catch (NumberFormatException e) {
        // Skip if not a valid number
      }
    }
  }

  /**
   * Set a boolean dimension using self-describing format: {@code dim/bool/{field}}
   *
   * <p>Supports migration from plain field names to slash-delimited format.
   *
   * @param file The FileRecord to populate
   * @param root The JSON object
   * @param jsonField The source field name
   */
  protected void setBoolDimension(FileRecord file, ObjectNode root, String jsonField) {
    String key = String.format("dim/bool/%s", jsonField);

    // Try slash-delimited format first (new format)
    if (root.has(key) && !root.get(key).isNull()) {
      boolean value = root.get(key).asBoolean();
      file.setDimension(key, value);
      return;
    }

    // Fallback to plain field name (legacy format)
    if (root.has(jsonField) && !root.get(jsonField).isNull()) {
      boolean value = root.get(jsonField).asBoolean();
      file.setDimension(key, value);
    }
  }

  /**
   * Set a float dimension using self-describing format: {@code dim/float/{field}}
   *
   * <p>Supports migration from plain field names to slash-delimited format.
   *
   * @param file The FileRecord to populate
   * @param root The JSON object
   * @param jsonField The source field name
   */
  protected void setFloatDimension(FileRecord file, ObjectNode root, String jsonField) {
    String key = String.format("dim/float/%s", jsonField);

    // Try slash-delimited format first (new format)
    if (root.has(key) && !root.get(key).isNull()) {
      JsonNode node = root.get(key);
      try {
        double value = node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText());
        file.setDimension(key, value);
      } catch (NumberFormatException e) {
        // Skip if not a valid number
      }
      return;
    }

    // Fallback to plain field name (legacy format)
    if (root.has(jsonField) && !root.get(jsonField).isNull()) {
      JsonNode node = root.get(jsonField);
      try {
        double value = node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText());
        file.setDimension(key, value);
      } catch (NumberFormatException e) {
        // Skip if not a valid number
      }
    }
  }

  // === Aggregation helpers (self-describing slash-delimited format) ===

  /**
   * Set an integer aggregation using self-describing format: {@code agg_int/{type}/{field}} or
   * {@code agg_int/{type}/{field}/{qualifier}}
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code agg_int/eq/record_count} - exact count
   *   <li>{@code agg_int/gte/level/warn} - count of records at warn level or above
   * </ul>
   *
   * @param file The FileRecord to populate
   * @param aggType Aggregation type (eq, gte, gt, lte, lt)
   * @param field Field name
   * @param qualifier Optional qualifier (can be null)
   * @param value The aggregation value
   */
  protected void setIntAgg(
      FileRecord file, String aggType, String field, String qualifier, long value) {
    String key;
    if (qualifier == null || qualifier.isEmpty()) {
      key = String.format("agg_int/%s/%s", aggType, field);
    } else {
      key = String.format("agg_int/%s/%s/%s", aggType, field, qualifier);
    }
    file.setIntAgg(key, value);
  }

  /**
   * Set a float aggregation using self-describing format: {@code agg_float/{type}/{field}} or
   * {@code agg_float/{type}/{field}/{qualifier}}
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code agg_float/sum/cpu/usage} - sum of CPU usage
   *   <li>{@code agg_float/avg/latency} - average latency
   * </ul>
   *
   * @param file The FileRecord to populate
   * @param aggType Aggregation type (sum, avg, min, max)
   * @param field Field name
   * @param qualifier Optional qualifier (can be null)
   * @param value The aggregation value
   */
  protected void setFloatAgg(
      FileRecord file, String aggType, String field, String qualifier, double value) {
    String key;
    if (qualifier == null || qualifier.isEmpty()) {
      key = String.format("agg_float/%s/%s", aggType, field);
    } else {
      key = String.format("agg_float/%s/%s/%s", aggType, field, qualifier);
    }
    file.setFloatAgg(key, value);
  }

  // === Field extraction helpers ===

  /**
   * Get a required string field, throwing if missing.
   *
   * @param root The JSON object
   * @param field The field name
   * @return The field value
   * @throws RecordTransformException if field is missing or null
   */
  protected String getRequiredString(ObjectNode root, String field)
      throws RecordTransformException {
    if (!root.has(field) || root.get(field).isNull()) {
      throw new RecordTransformException("Missing required field: " + field);
    }
    return root.get(field).asText();
  }

  /**
   * Get a required long field, throwing if missing.
   *
   * @param root The JSON object
   * @param field The field name
   * @return The field value
   * @throws RecordTransformException if field is missing or null
   */
  protected long getRequiredLong(ObjectNode root, String field) throws RecordTransformException {
    if (!root.has(field) || root.get(field).isNull()) {
      throw new RecordTransformException("Missing required field: " + field);
    }
    return root.get(field).asLong();
  }

  /**
   * Get an optional string field.
   *
   * @param root The JSON object
   * @param field The field name
   * @param defaultValue Default value if field is missing or null
   * @return The field value or default
   */
  protected String getOptionalString(ObjectNode root, String field, String defaultValue) {
    if (root.has(field) && !root.get(field).isNull()) {
      return root.get(field).asText();
    }
    return defaultValue;
  }

  /**
   * Get an optional long field.
   *
   * @param root The JSON object
   * @param field The field name
   * @param defaultValue Default value if field is missing or null
   * @return The field value or default
   */
  protected long getOptionalLong(ObjectNode root, String field, long defaultValue) {
    if (root.has(field) && !root.get(field).isNull()) {
      return root.get(field).asLong();
    }
    return defaultValue;
  }

  // === JSON key normalization ===

  /**
   * Normalize all JSON keys to lowercase.
   *
   * <p>Creates a defensive copy with lowercased keys. Useful for handling case-insensitive field
   * matching.
   *
   * @param parsed The parsed JSON node
   * @param objectMapper ObjectMapper for node creation
   * @return ObjectNode with lowercased keys
   */
  protected ObjectNode normalizeKeys(JsonNode parsed, ObjectMapper objectMapper) {
    ObjectNode normalized = objectMapper.createObjectNode();
    Iterator<Map.Entry<String, JsonNode>> fields = parsed.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      normalized.set(entry.getKey().toLowerCase(), entry.getValue());
    }
    return normalized;
  }
}
