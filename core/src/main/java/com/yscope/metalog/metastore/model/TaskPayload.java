package com.yscope.metalog.metastore.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

/**
 * Payload for tasks stored in the _task_queue table.
 *
 * <p>Contains all information needed by a worker to execute a consolidation task. Serialized as
 * LZ4-compressed msgpack and stored in the {@code input} MEDIUMBLOB column.
 *
 * <p>Wire format:
 *
 * <pre>
 * [4 bytes: uncompressed length (big-endian)] [LZ4 compressed msgpack]
 * </pre>
 *
 * <p>Msgpack layout (array of 7 elements):
 *
 * <pre>
 * [archivePath (string), tableName (string), irFilePaths (array of strings),
 *  irStorageBackend (string), irBucket (string),
 *  archiveStorageBackend (string), archiveBucket (string)]
 * </pre>
 *
 * <p>Fields:
 *
 * <ul>
 *   <li>archivePath - Destination object key for the consolidated archive
 *   <li>tableName - Target metadata table name
 *   <li>irFilePaths - List of IR file paths to consolidate
 *   <li>irStorageBackend - Storage backend type for IR files (e.g. "minio", "s3", "gcs")
 *   <li>irBucket - Bucket where IR files live
 *   <li>archiveStorageBackend - Storage backend type for archives (e.g. "minio", "s3", "gcs")
 *   <li>archiveBucket - Bucket where the archive should be uploaded
 * </ul>
 */
public class TaskPayload {
  private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();

  /** Maximum allowed uncompressed payload size (16 MB). */
  private static final int MAX_UNCOMPRESSED_SIZE = 16 * 1024 * 1024;

  private final String archivePath;
  private final List<String> irFilePaths;
  private final String tableName;
  private final String irStorageBackend;
  private final String irBucket;
  private final String archiveStorageBackend;
  private final String archiveBucket;

  public TaskPayload(
      String archivePath,
      List<String> irFilePaths,
      String tableName,
      String irStorageBackend,
      String irBucket,
      String archiveStorageBackend,
      String archiveBucket) {
    this.archivePath = Objects.requireNonNull(archivePath, "archivePath is required");
    this.irFilePaths = List.copyOf(Objects.requireNonNull(irFilePaths, "irFilePaths is required"));
    this.tableName = Objects.requireNonNull(tableName, "tableName is required");
    this.irStorageBackend =
        Objects.requireNonNull(irStorageBackend, "irStorageBackend is required");
    this.irBucket = irBucket; // nullable for non-bucket backends (local, http)
    this.archiveStorageBackend =
        Objects.requireNonNull(archiveStorageBackend, "archiveStorageBackend is required");
    this.archiveBucket = archiveBucket; // nullable for non-bucket backends (local, http)
  }

  public String getArchivePath() {
    return archivePath;
  }

  public List<String> getIrFilePaths() {
    return irFilePaths;
  }

  public String getTableName() {
    return tableName;
  }

  public String getIrStorageBackend() {
    return irStorageBackend;
  }

  public String getIrBucket() {
    return irBucket;
  }

  public String getArchiveStorageBackend() {
    return archiveStorageBackend;
  }

  public String getArchiveBucket() {
    return archiveBucket;
  }

  /** Get the number of IR files in this task. */
  public int getFileCount() {
    return irFilePaths.size();
  }

  /**
   * Serialize this payload to LZ4-compressed msgpack bytes for storage.
   *
   * @return compressed bytes with 4-byte uncompressed length prefix
   * @throws RuntimeException if serialization fails
   */
  public byte[] serialize() {
    try {
      // Pack to msgpack
      byte[] raw;
      try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
        packer.packArrayHeader(7);
        packer.packString(archivePath);
        packer.packString(tableName);
        packer.packArrayHeader(irFilePaths.size());
        for (String path : irFilePaths) {
          packer.packString(path);
        }
        packer.packString(irStorageBackend);
        if (irBucket != null) {
          packer.packString(irBucket);
        } else {
          packer.packNil();
        }
        packer.packString(archiveStorageBackend);
        if (archiveBucket != null) {
          packer.packString(archiveBucket);
        } else {
          packer.packNil();
        }
        raw = packer.toByteArray();
      }

      // Compress with LZ4
      LZ4Compressor compressor = LZ4.fastCompressor();
      int maxCompressedLen = compressor.maxCompressedLength(raw.length);
      byte[] compressed = new byte[maxCompressedLen];
      int compressedLen = compressor.compress(raw, 0, raw.length, compressed, 0, maxCompressedLen);

      // Prepend 4-byte big-endian uncompressed length
      ByteBuffer out = ByteBuffer.allocate(4 + compressedLen);
      out.putInt(raw.length);
      out.put(compressed, 0, compressedLen);
      return out.array();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize TaskPayload", e);
    }
  }

  /**
   * Deserialize a payload from LZ4-compressed msgpack bytes.
   *
   * @param bytes compressed bytes with 4-byte uncompressed length prefix
   * @return TaskPayload instance
   * @throws RuntimeException if deserialization fails
   */
  public static TaskPayload deserialize(byte[] bytes) {
    try {
      // Read 4-byte uncompressed length
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      int uncompressedLen = buf.getInt();

      if (uncompressedLen < 0 || uncompressedLen > MAX_UNCOMPRESSED_SIZE) {
        throw new IOException(
            "Invalid uncompressed size: "
                + uncompressedLen
                + " (max "
                + MAX_UNCOMPRESSED_SIZE
                + ")");
      }

      // Decompress
      LZ4FastDecompressor decompressor = LZ4.fastDecompressor();
      byte[] raw = new byte[uncompressedLen];
      decompressor.decompress(bytes, 4, raw, 0, uncompressedLen);

      // Unpack msgpack
      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(raw)) {
        unpacker.unpackArrayHeader(); // 7-element array
        String archivePath = unpacker.unpackString();
        String tableName = unpacker.unpackString();
        int pathCount = unpacker.unpackArrayHeader();
        List<String> irFilePaths = new ArrayList<>(pathCount);
        for (int i = 0; i < pathCount; i++) {
          irFilePaths.add(unpacker.unpackString());
        }
        String irStorageBackend = unpacker.unpackString();
        String irBucket =
            unpacker.tryUnpackNil() ? null : unpacker.unpackString();
        String archiveStorageBackend = unpacker.unpackString();
        String archiveBucket =
            unpacker.tryUnpackNil() ? null : unpacker.unpackString();
        return new TaskPayload(
            archivePath,
            irFilePaths,
            tableName,
            irStorageBackend,
            irBucket,
            archiveStorageBackend,
            archiveBucket);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize TaskPayload", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskPayload that = (TaskPayload) o;
    return Objects.equals(archivePath, that.archivePath)
        && Objects.equals(irFilePaths, that.irFilePaths)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(irStorageBackend, that.irStorageBackend)
        && Objects.equals(irBucket, that.irBucket)
        && Objects.equals(archiveStorageBackend, that.archiveStorageBackend)
        && Objects.equals(archiveBucket, that.archiveBucket);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        archivePath,
        irFilePaths,
        tableName,
        irStorageBackend,
        irBucket,
        archiveStorageBackend,
        archiveBucket);
  }

  @Override
  public String toString() {
    return String.format(
        "TaskPayload{archivePath='%s', fileCount=%d, tableName='%s', "
            + "irBackend='%s', irBucket='%s', archiveBackend='%s', archiveBucket='%s'}",
        archivePath,
        irFilePaths.size(),
        tableName,
        irStorageBackend,
        irBucket,
        archiveStorageBackend,
        archiveBucket);
  }
}
