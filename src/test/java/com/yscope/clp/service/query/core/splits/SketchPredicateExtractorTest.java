package com.yscope.clp.service.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

class SketchPredicateExtractorTest {

  // --- Pass-through: null / empty ---

  @ParameterizedTest
  @NullAndEmptySource
  void nullAndEmpty_returnUnchanged(String input) {
    SketchExtractionResult result = SketchPredicateExtractor.extract(input);
    assertEquals(input, result.cleanedExpression());
    assertTrue(result.predicates().isEmpty());
  }

  // --- No sketch predicates ---

  @Test
  void noSketchPredicates_returnUnchangedExpression() {
    String expr = "state = 'CLOSED'";
    SketchExtractionResult result = SketchPredicateExtractor.extract(expr);
    assertEquals("state = 'CLOSED'", result.cleanedExpression());
    assertTrue(result.predicates().isEmpty());
  }

  // --- Single equality ---

  @Test
  void singleEquality_extractedAndCleanedIsNull() {
    SketchExtractionResult result = SketchPredicateExtractor.extract("__SKETCH.user_id = 'abc'");
    assertNull(result.cleanedExpression());
    assertEquals(1, result.predicates().size());
    SketchPredicate pred = result.predicates().get(0);
    assertEquals("user_id", pred.field());
    assertEquals(Set.of("abc"), pred.values());
  }

  // --- IN expression ---

  @Test
  void inExpression_extractedWithAllValues() {
    SketchExtractionResult result =
        SketchPredicateExtractor.extract("__SKETCH.user_id IN ('a', 'b')");
    assertNull(result.cleanedExpression());
    assertEquals(1, result.predicates().size());
    SketchPredicate pred = result.predicates().get(0);
    assertEquals("user_id", pred.field());
    assertEquals(Set.of("a", "b"), pred.values());
  }

  // --- Mixed: sketch + regular ---

  @Test
  void mixedSketchAndRegular_predicateExtractedCleanedRetainsRegular() {
    SketchExtractionResult result =
        SketchPredicateExtractor.extract("state = 'CLOSED' AND __SKETCH.user_id = 'abc'");
    assertEquals("state = 'CLOSED'", result.cleanedExpression());
    assertEquals(1, result.predicates().size());
    assertEquals("user_id", result.predicates().get(0).field());
    assertEquals(Set.of("abc"), result.predicates().get(0).values());
  }

  // --- Two sketch predicates ---

  @Test
  void twoSketchPredicates_bothExtractedCleanedIsNull() {
    SketchExtractionResult result =
        SketchPredicateExtractor.extract("__SKETCH.user_id = 'a' AND __SKETCH.trace_id = 'b'");
    assertNull(result.cleanedExpression());
    assertEquals(2, result.predicates().size());
    List<String> fields = result.predicates().stream().map(SketchPredicate::field).toList();
    assertTrue(fields.contains("user_id"));
    assertTrue(fields.contains("trace_id"));
  }

  // --- Sketch + regular + sketch ---

  @Test
  void sketchRegularSketch_bothExtractedCleanedRetainsRegular() {
    SketchExtractionResult result =
        SketchPredicateExtractor.extract(
            "__SKETCH.a = 'x' AND max_timestamp > 0 AND __SKETCH.b = 'y'");
    assertEquals(2, result.predicates().size());
    List<String> fields = result.predicates().stream().map(SketchPredicate::field).toList();
    assertTrue(fields.contains("a"));
    assertTrue(fields.contains("b"));
    String cleaned = result.cleanedExpression();
    assertNotNull(cleaned);
    assertTrue(
        cleaned.contains("max_timestamp"), "Regular predicate missing from cleaned: " + cleaned);
    assertFalse(cleaned.contains("__SKETCH"), "Sketch term present in cleaned: " + cleaned);
  }

  // --- OR rejection ---

  @Test
  void sketchInsideOr_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SketchPredicateExtractor.extract("__SKETCH.user_id = 'abc' OR state = 'CLOSED'"));
  }

  @Test
  void sketchInsideNestedOr_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SketchPredicateExtractor.extract(
                "state = 'OPEN' AND (__SKETCH.user_id = 'a' OR state = 'CLOSED')"));
  }

  // --- NOT rejection ---

  @Test
  void sketchInsideNot_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SketchPredicateExtractor.extract("NOT __SKETCH.user_id = 'abc'"));
  }

  // --- Wrong operator ---

  @Test
  void wrongOperator_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SketchPredicateExtractor.extract("__SKETCH.user_id > 5"));
  }

  // --- Non-string IN items ---

  @Test
  void inWithNonStringItems_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SketchPredicateExtractor.extract("__SKETCH.user_id IN (1, 2, 3)"));
  }
}
