package tbft

import (
	"time"
)

type ConsensusTbftCost struct {
	ID              uint      `gorm:"type:int;primarykey"`
	Tbfttimestamp   time.Time `gorm:"index"`
	Height          uint64
	Round           int32
	Proposal        int64
	Prevote         int64
	Precommit       int64
	Commit          int64
	RoundTotalTime  int64
	HeightTotalTime int64
}
