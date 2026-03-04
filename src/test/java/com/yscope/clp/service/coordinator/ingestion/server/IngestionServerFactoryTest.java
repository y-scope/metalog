package com.yscope.clp.service.coordinator.ingestion.server;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("IngestionServerFactory")
class IngestionServerFactoryTest {

  @Test
  void disabled_throwsOnCreate() {
    IngestionServerFactory factory = IngestionServerFactory.disabled();
    assertThrows(UnsupportedOperationException.class, () -> factory.create(9091, null));
  }
}
