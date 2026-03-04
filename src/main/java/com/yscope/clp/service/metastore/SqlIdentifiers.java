package com.yscope.clp.service.metastore;

import java.util.regex.Pattern;

/** Validates SQL identifiers (table names, column names) to prevent injection. */
public final class SqlIdentifiers {

  /** Allows lowercase letters, digits, and underscores. Must start with letter or underscore. */
  private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-z_][a-z0-9_]{0,63}$");

  private SqlIdentifiers() {}

  /**
   * Validate that a string is a safe SQL identifier.
   *
   * @param identifier the identifier to validate
   * @param label descriptive label for error messages (e.g., "table name")
   * @return the identifier (for chaining)
   * @throws IllegalArgumentException if the identifier is invalid
   */
  public static String requireValid(String identifier, String label) {
    if (identifier == null || identifier.isEmpty()) {
      throw new IllegalArgumentException(label + " must not be null or empty");
    }
    if (!VALID_IDENTIFIER.matcher(identifier).matches()) {
      throw new IllegalArgumentException(
          label
              + " contains invalid characters: '"
              + identifier
              + "'"
              + " (must match "
              + VALID_IDENTIFIER.pattern()
              + ")");
    }
    return identifier;
  }

  /**
   * Validate a table name.
   *
   * @param tableName the table name to validate
   * @return the table name (for chaining)
   * @throws IllegalArgumentException if invalid
   */
  public static String requireValidTableName(String tableName) {
    return requireValid(tableName, "table name");
  }
}
