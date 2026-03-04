package com.yscope.clp.service.coordinator.ingestion;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.coordinator.grpc.proto.AggEntry;
import com.yscope.clp.service.coordinator.grpc.proto.AggType;
import com.yscope.clp.service.coordinator.grpc.proto.ArchiveFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.DimEntry;
import com.yscope.clp.service.coordinator.grpc.proto.DimensionValue;
import com.yscope.clp.service.coordinator.grpc.proto.FileFields;
import com.yscope.clp.service.coordinator.grpc.proto.IrFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataRecord;
import com.yscope.clp.service.coordinator.grpc.proto.SelfDescribingEntry;
import com.yscope.clp.service.coordinator.grpc.proto.StringDimension;
import com.yscope.clp.service.metastore.model.FileRecord;
import org.junit.jupiter.api.Test;

class ProtoConverterTest {

  private static MetadataRecord minimalRecord() {
    return MetadataRecord.newBuilder()
        .setFile(
            FileFields.newBuilder()
                .setState("IR_ARCHIVE_BUFFERING")
                .setMinTimestamp(1000)
                .setMaxTimestamp(2000)
                .setIr(
                    IrFileInfo.newBuilder()
                        .setClpIrStorageBackend("s3")
                        .setClpIrBucket("bucket")
                        .setClpIrPath("/path/file.clp")
                        .build())
                .build())
        .build();
  }

  // ==========================================
  // FileFields mapping
  // ==========================================

  @Test
  void testToFileRecord_fileFields_mapped() {
    FileRecord file = ProtoConverter.toFileRecord(minimalRecord());

    assertEquals("IR_ARCHIVE_BUFFERING", file.getState().name());
    assertEquals(1000, file.getMinTimestamp());
    assertEquals(2000, file.getMaxTimestamp());
    assertEquals("s3", file.getIrStorageBackend());
    assertEquals("bucket", file.getIrBucket());
    assertEquals("/path/file.clp", file.getIrPath());
  }

  @Test
  void testToFileRecord_archiveInfo_mapped() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(
                FileFields.newBuilder()
                    .setState("IR_ARCHIVE_BUFFERING")
                    .setArchive(
                        ArchiveFileInfo.newBuilder()
                            .setClpArchiveStorageBackend("gcs")
                            .setClpArchiveBucket("archive-bucket")
                            .setClpArchivePath("/archive/file.clp.zst")
                            .setClpArchiveCreatedAt(9999)
                            .setClpArchiveSizeBytes(512)
                            .build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals("gcs", file.getArchiveStorageBackend());
    assertEquals("archive-bucket", file.getArchiveBucket());
    assertEquals("/archive/file.clp.zst", file.getArchivePath());
    assertEquals(9999, file.getArchiveCreatedAt());
    assertEquals(512L, file.getArchiveSizeBytes());
  }

  @Test
  void testToFileRecord_absentArchiveInfo_nullFields() {
    FileRecord file = ProtoConverter.toFileRecord(minimalRecord());

    assertNull(file.getArchivePath());
    assertNull(file.getArchiveSizeBytes());
  }

  // ==========================================
  // Structured DimEntry
  // ==========================================

  @Test
  void testToFileRecord_dimEntry_str_buildsCompositeKey() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addDim(
                DimEntry.newBuilder()
                    .setKey("application_id")
                    .setValue(
                        DimensionValue.newBuilder()
                            .setStr(
                                StringDimension.newBuilder()
                                    .setValue("my-app")
                                    .setMaxLength(64)
                                    .build())
                            .build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals("my-app", file.getDimension("application_id" + '\0' + "str" + '\0' + 64));
  }

  @Test
  void testToFileRecord_dimEntry_strUtf8_buildsCompositeKey() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addDim(
                DimEntry.newBuilder()
                    .setKey("user_name")
                    .setValue(
                        DimensionValue.newBuilder()
                            .setStrUtf8(
                                StringDimension.newBuilder()
                                    .setValue("alice")
                                    .setMaxLength(128)
                                    .build())
                            .build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals("alice", file.getDimension("user_name" + '\0' + "str_utf8" + '\0' + 128));
  }

  @Test
  void testToFileRecord_dimEntry_str_defaultWidth255WhenMaxLengthZero() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addDim(
                DimEntry.newBuilder()
                    .setKey("field")
                    .setValue(
                        DimensionValue.newBuilder()
                            .setStr(StringDimension.newBuilder().setValue("val").build())
                            .build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals("val", file.getDimension("field" + '\0' + "str" + '\0' + 255));
  }

  @Test
  void testToFileRecord_dimEntry_int_mapped() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addDim(
                DimEntry.newBuilder()
                    .setKey("shard_id")
                    .setValue(DimensionValue.newBuilder().setIntVal(42).build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals(42L, file.getDimension("shard_id" + '\0' + "int" + '\0' + 0));
  }

  @Test
  void testToFileRecord_dimEntry_bool_mapped() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addDim(
                DimEntry.newBuilder()
                    .setKey("is_retry")
                    .setValue(DimensionValue.newBuilder().setBoolVal(true).build())
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals(true, file.getDimension("is_retry" + '\0' + "bool" + '\0' + 0));
  }

  // ==========================================
  // Structured AggEntry
  // ==========================================

  @Test
  void testToFileRecord_aggEntry_int_storedAsCompositeKey() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addAgg(
                AggEntry.newBuilder()
                    .setField("level")
                    .setQualifier("debug")
                    .setAggType(AggType.GTE)
                    .setIntVal(12345)
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    // Composite key: AGGTYPE\0field\0qualifier
    assertEquals(12345L, file.getCount("GTE\0level\0debug"));
  }

  @Test
  void testToFileRecord_aggEntry_float_storedAsCompositeKey() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addAgg(
                AggEntry.newBuilder()
                    .setField("latency")
                    .setQualifier("p99")
                    .setAggType(AggType.AVG)
                    .setFloatVal(1.5)
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals(1.5, file.getFloatCount("AVG\0latency\0p99"));
  }

  @Test
  void testToFileRecord_aggEntry_noQualifier_emptyQualifierInKey() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addAgg(
                AggEntry.newBuilder()
                    .setField("record_count")
                    .setAggType(AggType.EQ)
                    .setIntVal(999)
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals(999L, file.getCount("EQ\0record_count\0"));
  }

  // ==========================================
  // SelfDescribingEntry — dim keys
  // ==========================================

  @Test
  void testToFileRecord_selfDescribingDim_strPrefix_returnsString() {
    assertEquals("my-service", ProtoConverter.parseDimValue("dim/str128/service", "my-service"));
  }

  @Test
  void testToFileRecord_selfDescribingDim_intPrefix_returnsLong() {
    assertEquals(42L, ProtoConverter.parseDimValue("dim/int/port", "42"));
  }

  @Test
  void testToFileRecord_selfDescribingDim_floatPrefix_returnsDouble() {
    assertEquals(3.14, ProtoConverter.parseDimValue("dim/float/ratio", "3.14"));
  }

  @Test
  void testToFileRecord_selfDescribingDim_boolPrefix_returnsBoolean() {
    assertEquals(true, ProtoConverter.parseDimValue("dim/bool/active", "true"));
    assertEquals(false, ProtoConverter.parseDimValue("dim/bool/active", "false"));
  }

  @Test
  void testToFileRecord_selfDescribingDim_strUtf8_returnsString() {
    assertEquals("alice", ProtoConverter.parseDimValue("dim/str128utf8/user_name", "alice"));
  }

  // ==========================================
  // SelfDescribingEntry — agg keys
  // ==========================================

  @Test
  void testParseSelfDescAgg_intNoQualifier_stored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_int/eq/record_count", "500", file);

    assertEquals(500L, file.getCount("EQ\0record_count\0"));
  }

  @Test
  void testParseSelfDescAgg_intWithQualifier_stored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_int/gte/level/debug", "12345", file);

    assertEquals(12345L, file.getCount("GTE\0level\0debug"));
  }

  @Test
  void testParseSelfDescAgg_floatNoQualifier_stored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_float/avg/latency", "1.5", file);

    assertEquals(1.5, file.getFloatCount("AVG\0latency\0"));
  }

  @Test
  void testParseSelfDescAgg_floatWithQualifier_stored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_float/avg/latency/p99", "2.5", file);

    assertEquals(2.5, file.getFloatCount("AVG\0latency\0p99"));
  }

  @Test
  void testParseSelfDescAgg_invalidValue_ignored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_int/eq/record_count", "not-a-number", file);

    assertTrue(file.getCounts().isEmpty());
  }

  @Test
  void testParseSelfDescAgg_unknownAggType_ignored() {
    FileRecord file = new FileRecord();
    ProtoConverter.parseSelfDescAgg("agg_int/badtype/field", "100", file);

    assertTrue(file.getCounts().isEmpty());
  }

  @Test
  void testToFileRecord_selfDescribingKv_routedCorrectly() {
    MetadataRecord record =
        MetadataRecord.newBuilder()
            .setFile(FileFields.newBuilder().setState("IR_ARCHIVE_BUFFERING").build())
            .addSelfDescribingKv(
                SelfDescribingEntry.newBuilder()
                    .setKey("dim/str64/service")
                    .setValue("my-svc")
                    .build())
            .addSelfDescribingKv(
                SelfDescribingEntry.newBuilder()
                    .setKey("agg_int/gte/level/debug")
                    .setValue("999")
                    .build())
            .build();

    FileRecord file = ProtoConverter.toFileRecord(record);

    assertEquals("my-svc", file.getDimension("dim/str64/service"));
    assertEquals(999L, file.getCount("GTE\0level\0debug"));
  }
}
