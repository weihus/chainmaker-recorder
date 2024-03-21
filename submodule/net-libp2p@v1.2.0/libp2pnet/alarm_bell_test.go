package libp2pnet

import (
	"context"
	"testing"
	"time"
)

func TestAlarmBell(t *testing.T) {
	bell := newBell(sendDefaultTime, sendDefaultSize)
	go bell.start(context.Background())
	for i := 0; i < sendDefaultSize-1; i++ {
		if bell.needAlarm() {
			t.Error()
		}
	}
}

func TestAlarmBell2(t *testing.T) {
	bell := newBell(sendDefaultTime, sendDefaultSize)
	go bell.start(context.Background())

	count := 0
	for i := 0; i < sendDefaultSize*3; i++ {
		if bell.needAlarm() {
			count++
		}
	}
	if count != 1 {
		t.Error()
	}
}

func TestAlarmBell3(t *testing.T) {
	ts := time.Millisecond * 200
	bell := newBell(ts, sendDefaultSize)
	go bell.start(context.Background())

	count := 0
	for i := 0; i < sendDefaultSize*3; i++ {
		if bell.needAlarm() {
			count++
			time.Sleep(ts)
		}
	}
	if count <= 1 {
		t.Error(count)
	}
}
