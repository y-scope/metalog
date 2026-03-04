package com.yscope.metalog.metastore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for Columns constants and slash-format helpers.
 *
 * <p>Validates: - Column constant values match schema - Dimension helpers (dim, dimStr) -
 * Aggregation helpers (aggInt, aggFloat) - Validation logic for field names and types
 */
class ColumnsTest {

  // Dimension helper tests

  @Test
  void testDim_validInputs_returnsSlashDelimitedKey() {
    assertEquals("dim/str128/application_id", Columns.dim("str", 128, "application_id"));
    assertEquals("dim/str128/region", Columns.dim("str", 128, "region"));
    assertEquals("dim/int32/count", Columns.dim("int", 32, "count"));
    assertEquals("dim/uint16/port", Columns.dim("uint", 16, "port"));
    assertEquals("dim/float64/cpu_usage", Columns.dim("float", 64, "cpu_usage"));
  }

  @Test
  void testDimStr_validName_returnsCorrectKey() {
    assertEquals("dim/str128/application_id", Columns.dimStr(128, "application_id"));
    assertEquals("dim/str128/region", Columns.dimStr(128, "region"));
    assertEquals("dim/str128/service", Columns.dimStr(128, "service"));
  }

  @Test
  void testDim_invalidBaseType_throwsException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> Columns.dim("string", 128, "name"));

    assertTrue(exception.getMessage().contains("Invalid dimension base type"));
    assertTrue(exception.getMessage().contains("string"));
  }

  @Test
  void testDim_invalidWidth_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> Columns.dim("str", 0, "name"));
    assertThrows(IllegalArgumentException.class, () -> Columns.dim("str", -1, "name"));
  }

  @Test
  void testDim_invalidName_startsWithNumber_throwsException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> Columns.dimStr(128, "1invalid"));

    assertTrue(exception.getMessage().contains("Invalid dimension field"));
  }

  @Test
  void testDim_inputs_normalizedToLowercase() {
    assertEquals("dim/str128/invalidname", Columns.dimStr(128, "InvalidName"));
    assertEquals("dim/str128/application_id", Columns.dimStr(128, "APPLICATION_ID"));
    assertEquals("dim/str128/name", Columns.dim("STR", 128, "name"));
    assertEquals("dim/uint32/count", Columns.dim("UINT", 32, "count"));
    assertEquals("dim/float64/value", Columns.dim("Float", 64, "value"));
  }

  @Test
  void testDim_invalidName_specialChars_throwsException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> Columns.dimStr(128, "app-name"));

    assertTrue(exception.getMessage().contains("Invalid dimension field"));
  }

  // Aggregation helper tests

  @Test
  void testAggInt_validInputs_returnsSlashDelimitedKey() {
    assertEquals("agg_int/gte/level/debug", Columns.aggInt("gte", "level", "debug"));
    assertEquals("agg_int/eq/status/ok", Columns.aggInt("eq", "status", "ok"));
    assertEquals("agg_int/sum/bytes/total", Columns.aggInt("sum", "bytes", "total"));
  }

  @Test
  void testAggFloat_validInputs_returnsSlashDelimitedKey() {
    assertEquals("agg_float/avg/latency/p99", Columns.aggFloat("avg", "latency", "p99"));
    assertEquals("agg_float/sum/duration/total", Columns.aggFloat("sum", "duration", "total"));
  }

  @Test
  void testAgg_inputs_normalizedToLowercase() {
    assertEquals("agg_int/gte/level/debug", Columns.aggInt("GTE", "LEVEL", "DEBUG"));
    assertEquals("agg_int/eq/status/ok", Columns.aggInt("Eq", "Status", "Ok"));
    assertEquals("agg_float/avg/latency/p99", Columns.aggFloat("AVG", "LATENCY", "P99"));
  }

  @Test
  void testAllStandardColumns_immutable() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          Columns.ALL_STANDARD.add("new_column");
        });
  }
}
