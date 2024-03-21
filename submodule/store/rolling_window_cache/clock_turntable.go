package rolling_window_cache

import "time"

// ClockTurntable  a clock turntable
type ClockTurntable interface {
	// Get returns the value by given the time
	Get(t time.Time) uint64
	// GetLastSecond returns the sum of value of last n seconds
	GetLastSecond(n uint64) uint64
	// Add given n, that the value corresponding to the current moment, plus one n
	Add(n uint64)
}
