package timeutil

import "time"

// NanosPerSecond is the number of nanoseconds in one second (1e9).
const NanosPerSecond = int64(time.Second)

// EpochNanos returns the current time as epoch nanoseconds.
func EpochNanos() int64 {
	return time.Now().UnixNano()
}

// EpochSeconds returns the current time as epoch seconds (uint32).
func EpochSeconds() uint32 {
	return uint32(time.Now().Unix())
}

// NanosToTime converts epoch nanoseconds to time.Time.
func NanosToTime(nanos int64) time.Time {
	return time.Unix(0, nanos)
}

// TimeToNanos converts time.Time to epoch nanoseconds.
func TimeToNanos(t time.Time) int64 {
	return t.UnixNano()
}

// DayBoundaryNanos returns the epoch nanoseconds for the start of the UTC day
// containing the given epoch nanosecond timestamp.
func DayBoundaryNanos(nanos int64) int64 {
	t := NanosToTime(nanos).UTC()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return day.UnixNano()
}

// AddDaysNanos adds the given number of days to an epoch nanosecond timestamp.
func AddDaysNanos(nanos int64, days int) int64 {
	return NanosToTime(nanos).AddDate(0, 0, days).UnixNano()
}

// DayPartitionName returns a partition name like "p_20240115" for the given epoch nanos.
func DayPartitionName(nanos int64) string {
	t := NanosToTime(nanos).UTC()
	return "p_" + t.Format("20060102")
}
