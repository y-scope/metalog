use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};

/// Nanoseconds per second.
pub const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Nanoseconds per day.
pub const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

/// Returns the current time as epoch nanoseconds.
pub fn epoch_nanos() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Returns the current time as epoch seconds (u32).
pub fn epoch_seconds() -> u32 {
    Utc::now().timestamp() as u32
}

/// Converts epoch nanoseconds to a `DateTime<Utc>`.
pub fn nanos_to_datetime(nanos: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(nanos)
}

/// Converts a `DateTime<Utc>` to epoch nanoseconds.
pub fn datetime_to_nanos(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_nanos_opt().unwrap_or(0)
}

/// Returns the start of the UTC day (00:00:00) for the given epoch nanoseconds.
pub fn day_boundary_nanos(nanos: i64) -> i64 {
    let dt = nanos_to_datetime(nanos);
    let midnight = dt
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid for any date");
    Utc.from_utc_datetime(&midnight)
        .timestamp_nanos_opt()
        .unwrap_or(0)
}

/// Adds `days` to the given epoch nanoseconds.
pub fn add_days_nanos(nanos: i64, days: i32) -> i64 {
    nanos + i64::from(days) * NANOS_PER_DAY
}

/// Returns a partition name `p_YYYYMMDD` for the given epoch nanoseconds.
pub fn day_partition_name(nanos: i64) -> String {
    let dt = nanos_to_datetime(nanos);
    format!("p_{:04}{:02}{:02}", dt.year(), dt.month(), dt.day())
}

/// Returns the epoch nanoseconds for a given date (UTC midnight).
pub fn date_to_nanos(year: i32, month: u32, day: u32) -> i64 {
    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let midnight = date.and_hms_opt(0, 0, 0).unwrap();
    Utc.from_utc_datetime(&midnight)
        .timestamp_nanos_opt()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_nanos_positive() {
        let now = epoch_nanos();
        assert!(now > 0);
    }

    #[test]
    fn roundtrip_nanos() {
        let now = epoch_nanos();
        let dt = nanos_to_datetime(now);
        let back = datetime_to_nanos(dt);
        assert_eq!(now, back);
    }

    #[test]
    fn day_boundary() {
        // 2024-01-15 12:34:56.789 UTC
        let nanos = date_to_nanos(2024, 1, 15) + 12 * 3600 * NANOS_PER_SECOND;
        let boundary = day_boundary_nanos(nanos);
        let expected = date_to_nanos(2024, 1, 15);
        assert_eq!(boundary, expected);
    }

    #[test]
    fn add_days() {
        let base = date_to_nanos(2024, 1, 15);
        let result = add_days_nanos(base, 7);
        let expected = date_to_nanos(2024, 1, 22);
        assert_eq!(result, expected);
    }

    #[test]
    fn partition_name() {
        let nanos = date_to_nanos(2024, 1, 15);
        assert_eq!(day_partition_name(nanos), "p_20240115");
    }

    #[test]
    fn partition_name_with_zero_padding() {
        let nanos = date_to_nanos(2024, 3, 5);
        assert_eq!(day_partition_name(nanos), "p_20240305");
    }

    #[test]
    fn date_to_nanos_known() {
        // 2024-01-01 00:00:00 UTC = 1704067200 seconds
        let nanos = date_to_nanos(2024, 1, 1);
        assert_eq!(nanos, 1_704_067_200 * NANOS_PER_SECOND);
    }
}
