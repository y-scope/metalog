package com.yscope.clp.service.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class FilterExpressionValidatorTest {

  @Nested
  class ValidExpressions {

    @ParameterizedTest
    @NullAndEmptySource
    void nullAndEmpty_accepted(String input) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(input));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "min_timestamp <= 1679976000",
          "max_timestamp >= 1679711330",
          "record_count > 0",
          "record_count < 10000",
          "state = 'IR_CLOSED'",
          "state != 'IR_PURGING'",
        })
    void simpleComparisons(String expr) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(expr));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "a = 1 AND b = 2",
          "a = 1 OR b = 2",
          "a = 1 AND (b = 2 OR b = 3)",
          "(state = 'A' OR state = 'B') AND min_timestamp >= 100 AND record_count > 0",
        })
    void booleanLogic(String expr) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(expr));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "NOT (state = 'IR_PURGING')",
          "NOT state = 'IR_PURGING'",
        })
    void notExpressions(String expr) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(expr));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "state IN ('IR_CLOSED', 'ARCHIVE_CLOSED')",
          "record_count IN (100, 200, 300)",
        })
    void inExpressions(String expr) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(expr));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "state IS NULL",
          "state IS NOT NULL",
        })
    void isNullExpressions(String expr) {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate(expr));
    }

    @Test
    void betweenExpression() {
      assertDoesNotThrow(
          () -> FilterExpressionValidator.validate("record_count BETWEEN 1000 AND 5000"));
    }

    @Test
    void deeplyNested() {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate("((((state = 'IR_CLOSED'))))"));
    }

    @Test
    void complexCombined() {
      assertDoesNotThrow(
          () ->
              FilterExpressionValidator.validate(
                  "(state = 'A' OR state = 'B') AND min_timestamp >= 100"
                      + " AND record_count BETWEEN 1000 AND 5000"
                      + " AND max_timestamp IS NOT NULL"));
    }

    @Test
    void doubleValue() {
      assertDoesNotThrow(() -> FilterExpressionValidator.validate("ratio > 0.5"));
    }
  }

  @Nested
  class RejectedExpressions {

    @Test
    void subquery() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("state IN (SELECT state FROM t)"));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "SLEEP(5) = 0",
          "MD5(col) = 'abc'",
          "NOW() > min_timestamp",
        })
    void functionCalls(String expr) {
      assertThrows(IllegalArgumentException.class, () -> FilterExpressionValidator.validate(expr));
    }

    @Test
    void arithmetic() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("record_count + 100 > 1000"));
    }

    @Test
    void tableQualifiedColumn() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("clp_table.state = 'x'"));
    }

    @Test
    void caseExpression() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("CASE WHEN state = 'A' THEN 1 ELSE 0 END = 1"));
    }

    @Test
    void castExpression() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("CAST(record_count AS CHAR) = '100'"));
    }

    @Test
    void malformedSql() {
      assertThrows(
          IllegalArgumentException.class, () -> FilterExpressionValidator.validate("AND AND AND"));
    }

    @Test
    void semicolonInjection() {
      assertThrows(
          IllegalArgumentException.class,
          () -> FilterExpressionValidator.validate("1=1; DROP TABLE users"));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "0x414243 = 0x414243",
          "state = X'41'",
        })
    void hexLiterals(String expr) {
      // Hex literals are not in the whitelist and should be rejected.
      assertThrows(IllegalArgumentException.class, () -> FilterExpressionValidator.validate(expr));
    }
  }
}
