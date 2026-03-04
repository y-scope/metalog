package com.yscope.clp.service.query.api;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.common.config.db.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ApiServerConfig")
class ApiServerConfigTest {

  // ==========================================
  // fromNodeDatabaseConfig
  // ==========================================

  @Nested
  @DisplayName("fromNodeDatabaseConfig")
  class FromNodeDatabaseConfig {

    @Test
    void setsGrpcPort() {
      DatabaseConfig db = new DatabaseConfig();
      ApiServerConfig config = ApiServerConfig.fromNodeDatabaseConfig(db, 9999);

      assertEquals(9999, config.grpcPort());
    }

    @Test
    void setsJdbcUrl() {
      DatabaseConfig db = new DatabaseConfig();
      db.setHost("db.example.com");
      db.setPort(3307);
      db.setDatabase("mydb");

      ApiServerConfig config = ApiServerConfig.fromNodeDatabaseConfig(db, 9090);

      assertTrue(config.dataSource().jdbcUrl().contains("db.example.com"));
      assertTrue(config.dataSource().jdbcUrl().contains("3307"));
    }

    @Test
    void setsCredentials() {
      DatabaseConfig db = new DatabaseConfig();
      db.setUser("admin");
      db.setPassword("secret");

      ApiServerConfig config = ApiServerConfig.fromNodeDatabaseConfig(db, 9090);

      assertEquals("admin", config.dataSource().username());
      assertEquals("secret", config.dataSource().password());
    }
  }

  // ==========================================
  // DataSourceConfig.toHikariConfig
  // ==========================================

  @Nested
  @DisplayName("DataSourceConfig.toHikariConfig")
  class ToHikariConfig {

    @Test
    void populatesHikariConfigCorrectly() {
      var dsConfig =
          new ApiServerConfig.DataSourceConfig(
              "jdbc:mariadb://localhost:3306/test", "user", "pass", 10, true);

      HikariConfig hikari = dsConfig.toHikariConfig();

      assertEquals("jdbc:mariadb://localhost:3306/test", hikari.getJdbcUrl());
      assertEquals("user", hikari.getUsername());
      assertEquals("pass", hikari.getPassword());
      assertEquals(10, hikari.getMaximumPoolSize());
      assertTrue(hikari.isReadOnly());
      assertEquals("api-server-pool", hikari.getPoolName());
    }
  }

}
