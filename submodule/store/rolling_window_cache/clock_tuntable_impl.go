package rolling_window_cache

import (
	"sync"
	"time"
)

// ClockTurntableInstrument clock turntable implementation
type ClockTurntableInstrument struct {
	Status [60]*subStatus
}

// NewClockTurntableInstrument create a new instance
func NewClockTurntableInstrument() *ClockTurntableInstrument {
	status := [60]*subStatus{}

	for i := 0; i < 60; i++ {
		status[i] = &subStatus{
			idx:        0,
			createTime: time.Now(),
		}
	}
	c := &ClockTurntableInstrument{
		Status: status,
	}
	return c
}

// Get returns the value by given the time
func (C *ClockTurntableInstrument) Get(t time.Time) uint64 {
	i := t.Second() % 60
	return C.Status[i].Get(t)
}

// GetLastSecond returns the sum of value of last n seconds
func (C *ClockTurntableInstrument) GetLastSecond(n uint64) uint64 {
	var sum uint64
	var i uint64
	currentTime := time.Now()
	for i = 0; i <= n; i++ {
		beforeN := 0 - time.Duration(i)*time.Second
		beforeTime := currentTime.Add(beforeN)
		sum = sum + C.Get(beforeTime)
	}
	return sum
}

// Add given n, that the value corresponding to the current moment, plus one n
func (C *ClockTurntableInstrument) Add(n uint64) {
	current := time.Now()
	i := current.Second() % 60
	C.Status[i].Add(n)
}

type subStatus struct {
	idx        uint64
	createTime time.Time
	sync.RWMutex
}

// Get returns the value of given time
func (s *subStatus) Get(t time.Time) uint64 {
	current := t
	s.RLock()
	defer s.RUnlock()
	//expired
	if current.Sub(s.createTime) >= 1*time.Second || s.createTime.Sub(current) >= 1*time.Second {
		return 0
	}
	return s.idx
}

// Add given n, that the value corresponding to the current moment, plus one n
func (s *subStatus) Add(n uint64) {
	current := time.Now()
	s.Lock()
	defer s.Unlock()
	//expired
	if current.Sub(s.createTime) >= 1*time.Second {
		s.idx = n
		s.createTime = current
		return
	}
	//not expired
	s.idx = s.idx + n
}
