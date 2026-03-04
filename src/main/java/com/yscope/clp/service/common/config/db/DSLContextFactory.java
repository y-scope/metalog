package com.yscope.clp.service.common.config.db;

import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

/**
 * Factory for creating {@link DSLContext} instances with auto-detected SQL dialect.
 *
 * <p>Bridges the existing {@link DatabaseTypeDetector} to jOOQ's {@link SQLDialect}, producing a
 * DSLContext configured for the detected database. Uses prepared statements and unquoted
 * identifiers to match the existing hand-written SQL style.
 */
public final class DSLContextFactory {

  private DSLContextFactory() {}

  /**
   * Create a DSLContext with auto-detected dialect from the given DataSource.
   *
   * @param dataSource the connection pool to use
   * @return configured DSLContext
   */
  public static DSLContext create(DataSource dataSource) {
    SQLDialect dialect = detectDialect(dataSource);
    Settings settings =
        new Settings()
            .withRenderNameStyle(RenderNameStyle.AS_IS)
            .withRenderKeywordStyle(RenderKeywordStyle.UPPER)
            .withStatementType(StatementType.PREPARED_STATEMENT);
    return DSL.using(dataSource, dialect, settings);
  }

  /**
   * Detect the jOOQ SQLDialect from a DataSource using {@link DatabaseTypeDetector}.
   *
   * @param dataSource the DataSource to inspect
   * @return SQLDialect.MARIADB for MariaDB, SQLDialect.MYSQL for everything else
   */
  public static SQLDialect detectDialect(DataSource dataSource) {
    // DatabaseTypeDetector.detect() returns UNKNOWN on failure, so no try-catch needed.
    return DatabaseTypeDetector.detect(dataSource) == DatabaseTypeDetector.DatabaseType.MARIADB
        ? SQLDialect.MARIADB
        : SQLDialect.MYSQL;
  }
}
