package com.yscope.clp.service.metastore.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

/**
 * Result data persisted by a worker after completing a consolidation task.
 *
 * <p>Serialized as LZ4-compressed msgpack and stored in the {@code result} MEDIUMBLOB column of
 * {@code _task_queue}.
 *
 * <p>Wire format:
 *
 * <pre>
 * [4 bytes: uncompressed length (big-endian)] [LZ4 compressed msgpack]
 * </pre>
 *
 * <p>Msgpack layout (array of 2 elements):
 *
 * <pre>
 * [archiveSizeBytes (long), archiveCreatedAt (long)]
 * </pre>
 */
public class TaskResult {
  private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();

  /** Maximum allowed uncompressed result size (1 MB). */
  private static final int MAX_UNCOMPRESSED_SIZE = 1024 * 1024;

  private final long archiveSizeBytes;
  private final long archiveCreatedAt;

  /**
   * @param archiveSizeBytes actual archive size from {@code storageClient.createArchive()}
   * @param archiveCreatedAt epoch nanoseconds when the archive was created
   */
  public TaskResult(long archiveSizeBytes, long archiveCreatedAt) {
    this.archiveSizeBytes = archiveSizeBytes;
    this.archiveCreatedAt = archiveCreatedAt;
  }

  public long getArchiveSizeBytes() {
    return archiveSizeBytes;
  }

  public long getArchiveCreatedAt() {
    return archiveCreatedAt;
  }

  /**
   * Serialize this result to LZ4-compressed msgpack bytes for storage.
   *
   * @return compressed bytes with 4-byte uncompressed length prefix
   * @throws RuntimeException if serialization fails
   */
  public byte[] serialize() {
    try {
      byte[] raw;
      try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
        packer.packArrayHeader(2);
        packer.packLong(archiveSizeBytes);
        packer.packLong(archiveCreatedAt);
        raw = packer.toByteArray();
      }

      LZ4Compressor compressor = LZ4.fastCompressor();
      int maxCompressedLen = compressor.maxCompressedLength(raw.length);
      byte[] compressed = new byte[maxCompressedLen];
      int compressedLen = compressor.compress(raw, 0, raw.length, compressed, 0, maxCompressedLen);

      ByteBuffer out = ByteBuffer.allocate(4 + compressedLen);
      out.putInt(raw.length);
      out.put(compressed, 0, compressedLen);
      return out.array();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize TaskResult", e);
    }
  }

  /**
   * Deserialize a result from LZ4-compressed msgpack bytes.
   *
   * @param bytes compressed bytes with 4-byte uncompressed length prefix
   * @return TaskResult instance
   * @throws RuntimeException if deserialization fails
   */
  public static TaskResult deserialize(byte[] bytes) {
    try {
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

      LZ4FastDecompressor decompressor = LZ4.fastDecompressor();
      byte[] raw = new byte[uncompressedLen];
      decompressor.decompress(bytes, 4, raw, 0, uncompressedLen);

      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(raw)) {
        unpacker.unpackArrayHeader();
        long archiveSizeBytes = unpacker.unpackLong();
        long archiveCreatedAt = unpacker.unpackLong();
        return new TaskResult(archiveSizeBytes, archiveCreatedAt);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize TaskResult", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskResult that = (TaskResult) o;
    return archiveSizeBytes == that.archiveSizeBytes && archiveCreatedAt == that.archiveCreatedAt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(archiveSizeBytes, archiveCreatedAt);
  }

  @Override
  public String toString() {
    return String.format(
        "TaskResult{archiveSizeBytes=%d, archiveCreatedAt=%d}", archiveSizeBytes, archiveCreatedAt);
  }
}
