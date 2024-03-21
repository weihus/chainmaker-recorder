package rolling_window_cache

import (
	"testing"
	"time"
)

func TestClockTurntableInstrument_Get(t *testing.T) {

	//case1 want3
	want3 := uint64(3)
	c := NewClockTurntableInstrument()
	for i := 0; i < 3; i++ {
		c.Add(1)
	}
	if got := c.Get(time.Now()); got != want3 {
		t.Errorf("Get() = %v, want %v", got, want3)
	}

	//case2 want0
	want0 := uint64(0)
	preTime := time.Now()
	for i := 0; i < 4; i++ {
		c.Add(1)
		//fmt.Println("---------",C.Get(preTime))
		if i == 3 {
			time.Sleep(1 * time.Second)
			//fmt.Println("---------",C.Get(preTime))
		}
	}
	if got := c.Get(time.Now()); got != want0 {
		t.Errorf("Get() = %v, want %v", got, want0)
	}
	//case3 want7
	want7 := uint64(7)
	if got := c.Get(preTime); got != want7 {
		t.Errorf("Get() = %v, want %v", got, want7)
	}

	//case4 want11
	// after 60 seconds, the value of clock turntable  has been reset to 0
	time.Sleep(1 * time.Minute)
	for i := 0; i < 11; i++ {
		c.Add(1)
		time.Sleep(1 * time.Second)
	}
	want11 := uint64(11)
	if got := c.GetLastSecond(60); got != want11 {
		t.Errorf("Get() = %v, want %v", got, want11)
	}

}
