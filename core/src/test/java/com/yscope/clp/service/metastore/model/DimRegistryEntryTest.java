package com.yscope.clp.service.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DimRegistryEntry Unit Tests")
class DimRegistryEntryTest {

  private static DimRegistryEntry entry(String columnName, String aliasColumn) {
    return new DimRegistryEntry(
        "tbl", columnName, "str", 128, "key", aliasColumn, "ACTIVE", null, null);
  }

  @Test
  @DisplayName("isAlias returns false when aliasColumn is null")
  void testIsAlias_null() {
    assertFalse(entry("dim_f01", null).isAlias());
  }

  @Test
  @DisplayName("isAlias returns true when aliasColumn is set")
  void testIsAlias_set() {
    assertTrue(entry("clp_ir_storage_backend", "clp_ir_storage_backend").isAlias());
  }

  @Test
  @DisplayName("effectiveColumn returns columnName when not an alias")
  void testEffectiveColumn_notAlias() {
    assertEquals("dim_f01", entry("dim_f01", null).effectiveColumn());
  }

  @Test
  @DisplayName("effectiveColumn returns aliasColumn when alias")
  void testEffectiveColumn_alias() {
    assertEquals(
        "clp_ir_storage_backend",
        entry("clp_ir_storage_backend", "clp_ir_storage_backend").effectiveColumn());
  }
}
