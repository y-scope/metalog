package com.yscope.metalog.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.metalog.metastore.schema.BaseSchemaValidator.SchemaValidationException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("BaseSchemaValidator Unit Tests")
class BaseSchemaValidatorTest {

  private DataSource dataSource;
  private Connection connection;
  private PreparedStatement statement;
  private ResultSet resultSet;

  @BeforeEach
  void setUp() throws SQLException {
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(PreparedStatement.class);
    resultSet = mock(ResultSet.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
  }

  /** Configure the mock ResultSet to return the given column names one at a time. */
  private void mockColumns(String... columns) throws SQLException {
    // First call returns true for each column, then false
    if (columns.length == 0) {
      when(resultSet.next()).thenReturn(false);
      return;
    }

    Boolean[] subsequentCalls = new Boolean[columns.length];
    for (int i = 0; i < columns.length - 1; i++) {
      subsequentCalls[i] = true;
    }
    subsequentCalls[columns.length - 1] = false;
    when(resultSet.next()).thenReturn(true, subsequentCalls);

    // Return column names in sequence
    if (columns.length == 1) {
      when(resultSet.getString("COLUMN_NAME")).thenReturn(columns[0]);
    } else {
      String[] rest = new String[columns.length - 1];
      System.arraycopy(columns, 1, rest, 0, rest.length);
      when(resultSet.getString("COLUMN_NAME")).thenReturn(columns[0], rest);
    }
  }

  // ==========================================
  // REQUIRED_COLUMNS
  // ==========================================

  @Test
  void testRequiredColumns_containsExpectedColumns() {
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("id"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("min_timestamp"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("state"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("clp_ir_path"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("clp_archive_path"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("record_count"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("retention_days"));
    assertTrue(BaseSchemaValidator.REQUIRED_COLUMNS.contains("expires_at"));
    assertEquals(17, BaseSchemaValidator.REQUIRED_COLUMNS.size());
  }

  // ==========================================
  // validate — happy path
  // ==========================================

  @Nested
  @DisplayName("validate")
  class Validate {

    @Test
    void testValidate_allColumnsPresent_returnsDiscovered() throws Exception {
      // Include all required columns plus some extras
      String[] allColumns = new String[BaseSchemaValidator.REQUIRED_COLUMNS.size() + 2];
      for (int i = 0; i < BaseSchemaValidator.REQUIRED_COLUMNS.size(); i++) {
        allColumns[i] = BaseSchemaValidator.REQUIRED_COLUMNS.get(i);
      }
      allColumns[allColumns.length - 2] = "dim_f01";
      allColumns[allColumns.length - 1] = "agg_f01";

      mockColumns(allColumns);

      BaseSchemaValidator validator = new BaseSchemaValidator(dataSource, "test_table");
      Set<String> discovered = validator.validate();

      assertEquals(allColumns.length, discovered.size());
      assertTrue(discovered.contains("id"));
      assertTrue(discovered.contains("dim_f01"));
    }

    @Test
    void testValidate_missingColumns_throwsSchemaValidationException() throws Exception {
      // Only provide a subset of required columns
      mockColumns("id", "state", "record_count");

      BaseSchemaValidator validator = new BaseSchemaValidator(dataSource, "test_table");

      SchemaValidationException ex =
          assertThrows(SchemaValidationException.class, validator::validate);

      Set<String> missing = ex.getMissingColumns();
      assertTrue(missing.contains("min_timestamp"));
      assertTrue(missing.contains("clp_ir_path"));
      assertFalse(missing.contains("id"));
    }

    @Test
    void testValidate_emptyResultSet_throwsSQLException() throws Exception {
      mockColumns(); // empty — no columns returned

      BaseSchemaValidator validator = new BaseSchemaValidator(dataSource, "test_table");
      SQLException ex = assertThrows(SQLException.class, validator::validate);
      assertTrue(ex.getMessage().contains("not found or has no columns"));
    }

    @Test
    void testValidate_caseInsensitive() throws Exception {
      // DB returns uppercase; required columns are lowercase
      String[] allColumns = new String[BaseSchemaValidator.REQUIRED_COLUMNS.size()];
      for (int i = 0; i < BaseSchemaValidator.REQUIRED_COLUMNS.size(); i++) {
        allColumns[i] = BaseSchemaValidator.REQUIRED_COLUMNS.get(i).toUpperCase();
      }
      mockColumns(allColumns);

      BaseSchemaValidator validator = new BaseSchemaValidator(dataSource, "test_table");
      // Should pass because discoverColumns lowercases everything
      Set<String> discovered = validator.validate();
      assertFalse(discovered.isEmpty());
    }
  }

  // ==========================================
  // SchemaValidationException
  // ==========================================

  @Test
  void testSchemaValidationException_missingColumnsImmutable() {
    Set<String> missing = Set.of("col_a", "col_b");
    SchemaValidationException ex = new SchemaValidationException("test", missing);

    assertEquals(missing, ex.getMissingColumns());
    assertThrows(UnsupportedOperationException.class, () -> ex.getMissingColumns().add("col_c"));
  }
}
