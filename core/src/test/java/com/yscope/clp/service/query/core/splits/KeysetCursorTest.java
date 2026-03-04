package com.yscope.clp.service.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class KeysetCursorTest {

  @Test
  void recordFields_singleAndMultipleValues() {
    var single = new KeysetCursor(List.of(1704067200L), 42L);
    assertEquals(List.of(1704067200L), single.sortValues());
    assertEquals(42L, single.id());

    var multi = new KeysetCursor(List.of(1000L, 100L), 7L);
    assertEquals(List.of(1000L, 100L), multi.sortValues());
    assertEquals(7L, multi.id());
  }

  @Test
  void equality_andInequality() {
    var a = new KeysetCursor(List.of(100L), 1L);
    var b = new KeysetCursor(List.of(100L), 1L);
    var c = new KeysetCursor(List.of(100L), 2L);
    assertEquals(a, b);
    assertNotEquals(a, c);
  }
}
