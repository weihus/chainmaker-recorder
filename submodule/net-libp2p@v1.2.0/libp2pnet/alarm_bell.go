package libp2pnet

import (
	"context"
	"sync/atomic"
	"time"
)

type bell struct {
	alarmInterval time.Duration
	alarmCount    int32
	maxAlarmCount int32
	alreadyAlarm  bool
}

func newBell(alarmInterval time.Duration, maxAlarmCount int32) *bell {
	return &bell{
		alarmInterval: alarmInterval,
		maxAlarmCount: maxAlarmCount,
		alarmCount:    0,
		alreadyAlarm:  false,
	}
}

func (b *bell) start(ctx context.Context) {
	ticker := time.NewTicker(b.alarmInterval)
	for {
		select {
		case <-ticker.C:
			atomic.StoreInt32(&b.alarmCount, 0)
			b.alreadyAlarm = false
		case <-ctx.Done():
			return
		}
	}
}

func (b *bell) needAlarm() bool {
	n := atomic.AddInt32(&b.alarmCount, 1)
	// beyond maxAlarmCount and did not alarm in time space
	if n > b.maxAlarmCount && !b.alreadyAlarm {
		b.alreadyAlarm = true
		return true
	}
	return false
}
