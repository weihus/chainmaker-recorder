/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"encoding/json"
	"time"
)

//
// roundMetrics
// @Description: Record each stage status duration and start time
//
type roundMetrics struct {
	// current round
	round              int32
	enterNewRoundTime  time.Time
	enterProposalTime  time.Time
	enterPrevoteTime   time.Time
	enterPrecommitTime time.Time
	enterCommitTime    time.Time
	// Status duration for each stage
	persistStateDurations map[string][]time.Duration
}

//
// newRoundMetrics
// @Description: Enter a new round, create a new object（*roundMetrics）
// @param round
// @return *roundMetrics
//
func newRoundMetrics(round int32) *roundMetrics {
	return &roundMetrics{
		round:                 round,
		persistStateDurations: make(map[string][]time.Duration),
	}
}

//
// roundMetricsJson
// @Description: json format roundMetrics object
//
type roundMetricsJson struct {
	Round                 int32
	Proposal              string
	Prevote               string
	Precommit             string
	PersistStateDurations map[string][]string
}

//
// roundMetricsJson
// @Description: convert *roundMetrics to *roundMetricsJson
// @receiver r
// @return *roundMetricsJson
//
func (r *roundMetrics) roundMetricsJson() *roundMetricsJson {
	j := &roundMetricsJson{
		Round:                 r.round,
		Proposal:              r.enterPrevoteTime.Sub(r.enterProposalTime).String(),
		Prevote:               r.enterPrecommitTime.Sub(r.enterPrevoteTime).String(),
		Precommit:             r.enterCommitTime.Sub(r.enterPrecommitTime).String(),
		PersistStateDurations: make(map[string][]string),
	}

	for k, v := range r.persistStateDurations {
		for _, d := range v {
			j.PersistStateDurations[k] = append(j.PersistStateDurations[k], d.String())
		}
	}

	return j
}

func (r *roundMetrics) GetCost() *ConsensusTbftCost {
	now := time.Now()
	return &ConsensusTbftCost{
		Round:          r.round,
		Proposal:       int64(r.enterPrevoteTime.Sub(r.enterProposalTime)),
		Prevote:        int64(r.enterPrecommitTime.Sub(r.enterPrevoteTime)),
		Precommit:      int64(r.enterCommitTime.Sub(r.enterPrecommitTime)),
		Commit:         int64(now.Sub(r.enterCommitTime)),
		RoundTotalTime: int64(now.Sub(r.enterNewRoundTime)),
	}
}

func (r *roundMetrics) SetEnterNewRoundTime() {
	r.enterNewRoundTime = time.Now()
}

func (r *roundMetrics) SetEnterProposalTime() {
	r.enterProposalTime = time.Now()
}

func (r *roundMetrics) SetEnterPrevoteTime() {
	r.enterPrevoteTime = time.Now()
}

func (r *roundMetrics) SetEnterPrecommitTime() {
	r.enterPrecommitTime = time.Now()
}

func (r *roundMetrics) SetEnterCommitTime() {
	r.enterCommitTime = time.Now()
}

func (r *roundMetrics) AppendPersistStateDuration(step string, d time.Duration) {
	r.persistStateDurations[step] = append(r.persistStateDurations[step], d)
}

//
// heightMetrics
// @Description: The time of each stage of a certain height includes all rounds
//
type heightMetrics struct {
	height             uint64
	enterNewHeightTime time.Time
	rounds             map[int32]*roundMetrics
}

//
// newHeightMetrics
// @Description: enter new height create a *heightMetrics
// @param height
// @return *heightMetrics
//
func newHeightMetrics(height uint64) *heightMetrics {
	return &heightMetrics{
		height: height,
		rounds: make(map[int32]*roundMetrics),
	}
}

//
// heightMetricsJson
// @Description: json format heightMetrics object
//
type heightMetricsJson struct {
	Height             uint64
	EnterNewHeightTime string
	Rounds             map[int32]*roundMetricsJson
}

//
// String
// @Description: convert *heightMetrics to string
// @receiver h
// @return string
//
func (h *heightMetrics) String() string {
	j := heightMetricsJson{
		Height:             h.height,
		EnterNewHeightTime: h.enterNewHeightTime.String(),
		Rounds:             map[int32]*roundMetricsJson{},
	}
	for k, v := range h.rounds {
		j.Rounds[k] = v.roundMetricsJson()
	}
	byt, _ := json.Marshal(j)
	return string(byt)
}

//
// roundString
// @Description: convert a roundMetrics of heightMetrics to string
// @receiver h
// @param num
// @return string
//
func (h *heightMetrics) roundString(num int32) string {
	j := heightMetricsJson{
		Height:             h.height,
		EnterNewHeightTime: h.enterNewHeightTime.String(),
		Rounds:             map[int32]*roundMetricsJson{},
	}
	j.Rounds[num] = h.rounds[num].roundMetricsJson()
	byt, _ := json.Marshal(j)
	return string(byt)
}

func (h *heightMetrics) SetEnterNewHeightTime() {
	h.enterNewHeightTime = time.Now()
}

func (h *heightMetrics) getRoundMertrics(round int32) *roundMetrics {
	if _, ok := h.rounds[round]; !ok {
		h.rounds[round] = newRoundMetrics(round)
	}

	return h.rounds[round]
}

func (h *heightMetrics) SetEnterNewRoundTime(round int32) {
	r := h.getRoundMertrics(round)
	r.SetEnterNewRoundTime()
}

func (h *heightMetrics) SetEnterProposalTime(round int32) {
	r := h.getRoundMertrics(round)
	r.SetEnterProposalTime()
}

func (h *heightMetrics) SetEnterPrevoteTime(round int32) {
	r := h.getRoundMertrics(round)
	r.SetEnterPrevoteTime()
}

func (h *heightMetrics) SetEnterPrecommitTime(round int32) {
	r := h.getRoundMertrics(round)
	r.SetEnterPrecommitTime()
}

func (h *heightMetrics) SetEnterCommitTime(round int32) {
	r := h.getRoundMertrics(round)
	r.SetEnterCommitTime()
}

func (h *heightMetrics) AppendPersistStateDuration(round int32, step string, d time.Duration) {
	r := h.getRoundMertrics(round)
	r.AppendPersistStateDuration(step, d)
}

func (h *heightMetrics) GetCost(round int32) *ConsensusTbftCost {
	r := h.getRoundMertrics(round)
	cost := r.GetCost()
	cost.Height = h.height
	cost.HeightTotalTime = int64(time.Now().Sub(h.enterNewHeightTime))
	return cost
}
