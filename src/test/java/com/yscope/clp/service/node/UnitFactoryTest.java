package com.yscope.clp.service.node;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.yscope.clp.service.common.storage.ArchiveCreator;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for UnitFactory. */
class UnitFactoryTest {

  private SharedResources mockResources;
  private BatchingWriter mockBatchingWriter;

  @BeforeEach
  void setUp() {
    HikariDataSource mockDataSource = mock(HikariDataSource.class);
    StorageRegistry mockRegistry = mock(StorageRegistry.class);
    ArchiveCreator mockArchiveCreator = mock(ArchiveCreator.class);
    mockResources = new SharedResources(mockDataSource, mockRegistry, mockArchiveCreator);
    mockBatchingWriter = mock(BatchingWriter.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"unknown", "  "})
  void createUnit_unrecognizedType_throws(String type) {
    NodeConfig.UnitDefinition definition = new NodeConfig.UnitDefinition();
    definition.setName("test");
    definition.setType(type);

    assertThrows(
        IllegalArgumentException.class,
        () -> UnitFactory.createUnit(definition, mockResources, mockBatchingWriter));
  }
}
