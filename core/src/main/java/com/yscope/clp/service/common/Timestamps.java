package com.yscope.clp.service.common;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Utility class for nanosecond-precision epoch timestamps.
 *
 * <p>File-level timestamps ({@code min_timestamp}, {@code max_timestamp}, {@code expires_at},
 * {@code clp_archive_created_at}) use epoch nanoseconds (BIGINT in the database). This class
 * centralizes all time conversion logic so call sites never deal with raw arithmetic.
 *
 * <p>Operational timestamps (task queue, node registry, column registries) remain epoch seconds
 * because they don't need sub-second precision.
 */
public final class Timestamps {

  public static final long NANOS_PER_SECOND = 1_000_000_000L;
  public static final long NANOS_PER_MINUTE = 60L * NANOS_PER_SECOND;
  public static final long NANOS_PER_HOUR = 3_600L * NANOS_PER_SECOND;
  public static final long NANOS_PER_DAY = 86_400L * NANOS_PER_SECOND;

  /** Return the current time as epoch nanoseconds. */
  public static long nowNanos() {
    Instant now = Instant.now();
    return now.getEpochSecond() * NANOS_PER_SECOND + now.getNano();
  }

  /** Convert epoch seconds to epoch nanoseconds. */
  public static long secondsToNanos(long epochSeconds) {
    return epochSeconds * NANOS_PER_SECOND;
  }

  /** Convert epoch nanoseconds to epoch seconds (truncates sub-second part). */
  public static long nanosToSeconds(long epochNanos) {
    return epochNanos / NANOS_PER_SECOND;
  }

  /** Return the start of the given day (UTC midnight) as epoch nanoseconds. */
  public static long startOfDayNanos(LocalDate date) {
    return date.atStartOfDay(ZoneOffset.UTC).toEpochSecond() * NANOS_PER_SECOND;
  }

  /** Reconstruct an {@link Instant} from epoch nanoseconds. */
  public static Instant toInstant(long epochNanos) {
    long seconds = epochNanos / NANOS_PER_SECOND;
    int nanos = (int) (epochNanos % NANOS_PER_SECOND);
    return Instant.ofEpochSecond(seconds, nanos);
  }

  private Timestamps() {
    throw new UnsupportedOperationException("Utility class");
  }
}
