package com.yscope.clp.service.node;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigMapParserTest {

  @Test
  void nullMap_treatedAsEmpty() {
    ConfigMapParser parser = new ConfigMapParser(null);
    assertEquals("default", parser.getString("missing", "default"));
    assertFalse(parser.containsKey("anything"));
  }

  @Test
  void getString_presentAndAbsent() {
    ConfigMapParser parser = new ConfigMapParser(Map.of("host", "db.example.com", "port", 9090));
    assertEquals("db.example.com", parser.getString("host", "localhost"));
    // Missing key → default
    assertEquals("localhost", parser.getString("missing", "localhost"));
    // Wrong type → default
    assertEquals("localhost", parser.getString("port", "localhost"));
  }

  @Test
  void getInt_presentAbsentAndWrongType() {
    Map<String, Object> map = new HashMap<>();
    map.put("port", 9090);
    map.put("clp.test.not_a_number", "not-a-number");
    ConfigMapParser parser = new ConfigMapParser(map);
    assertEquals(9090, parser.getInt("port", 3306));
    assertEquals(3306, parser.getInt("missing", 3306));
    assertEquals(3306, parser.getInt("clp.test.not_a_number", 3306));
  }

  @Test
  void getLong_presentAndAbsent() {
    ConfigMapParser parser = new ConfigMapParser(Map.of("timeout", 60000L));
    assertEquals(60000L, parser.getLong("timeout", 30000L));
    assertEquals(30000L, parser.getLong("missing", 30000L));
  }

  @Test
  void getBoolean_presentAbsentAndWrongType() {
    ConfigMapParser parser = new ConfigMapParser(Map.of("enabled", true, "flag", "yes"));
    assertTrue(parser.getBoolean("enabled", false));
    assertFalse(parser.getBoolean("missing", false));
    assertFalse(parser.getBoolean("flag", false));
  }

  @Test
  void getMap_presentAbsentAndWrongType() {
    Map<String, Object> nested = Map.of("host", "localhost");
    ConfigMapParser parser = new ConfigMapParser(Map.of("database", nested, "name", "not-a-map"));
    assertEquals(nested, parser.getMap("database"));
    assertNull(parser.getMap("missing"));
    assertNull(parser.getMap("name"));
  }

  @Test
  void containsKey() {
    ConfigMapParser parser = new ConfigMapParser(Map.of("host", "value"));
    assertTrue(parser.containsKey("host"));
    assertFalse(parser.containsKey("missing"));
  }
}
