package com.yscope.clp.service.coordinator.ingestion;

import com.yscope.clp.service.metastore.SqlIdentifiers;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.schema.RecordStateValidator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Framework-agnostic ingestion service.
 *
 * <p>Validates domain objects ({@link FileRecord}), submits valid records to a {@link
 * BatchingWriter}, and returns a {@link CompletableFuture} that completes after the DB commit.
 *
 * <p>Protocol adapters (gRPC, HTTP, etc.) convert wire-format records to {@link FileRecord} and
 * delegate to this service. This keeps validation and batching logic independent of the transport
 * layer.
 */
public class IngestionService {
  private static final Logger logger = LoggerFactory.getLogger(IngestionService.class);

  private final BatchingWriter writer;
  private final RecordStateValidator validator = new RecordStateValidator();

  public IngestionService(BatchingWriter writer) {
    this.writer = writer;
  }

  /**
   * Validate and ingest a batch of IR files into a table.
   *
   * <p>Each file is validated via {@link RecordStateValidator}. Valid files are submitted to the
   * {@link BatchingWriter}; invalid files are collected into the error map.
   *
   * @param tableName target metadata table
   * @param files domain objects to validate and write
   * @return future that completes after DB commit with the ingestion result
   * @throws IllegalArgumentException if tableName is null or empty
   */
  public CompletableFuture<IngestionResult> ingest(String tableName, List<FileRecord> files) {
    SqlIdentifiers.requireValidTableName(tableName);

    List<FileRecord> validFiles = new ArrayList<>();
    Map<Integer, String> recordErrors = new HashMap<>();

    for (int i = 0; i < files.size(); i++) {
      FileRecord file = files.get(i);
      RecordStateValidator.ValidationResult result = validator.validate(file);
      if (result.isValid()) {
        validFiles.add(file);
      } else {
        recordErrors.put(i, String.join("; ", result.getErrors()));
      }
    }

    if (!recordErrors.isEmpty()) {
      logger.warn(
          "Rejected {}/{} records for table {} (validation failed)",
          recordErrors.size(),
          files.size(),
          tableName);
    }

    if (validFiles.isEmpty()) {
      return CompletableFuture.completedFuture(
          new IngestionResult(0, recordErrors.size(), recordErrors, 0));
    }

    return writer
        .submit(tableName, validFiles)
        .thenApply(
            writeResult ->
                new IngestionResult(
                    validFiles.size(),
                    recordErrors.size(),
                    recordErrors,
                    writeResult.rowsAffected()));
  }

  /** Result of an ingestion operation. */
  public record IngestionResult(
      int recordsAccepted,
      int recordsRejected,
      Map<Integer, String> recordErrors,
      int rowsAffected) {}
}
