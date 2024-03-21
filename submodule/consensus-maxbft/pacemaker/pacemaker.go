/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pacemaker

import (
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"

	timeservice "chainmaker.org/chainmaker/consensus-utils/v2/time_service"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

// PaceMaker defines interface of consensus pacemaker of maxBft,
// used to maintain the activity of consensus algorithms
type PaceMaker interface {
	Start()
	Stop()
	CurView() uint64
	TimeoutChannel() <-chan *timeservice.TimerEvent

	OnTimeout(event *timeservice.TimerEvent) *timeservice.TimerEvent
	UpdateWithQc(qc *maxbftpb.QuorumCert) (*timeservice.TimerEvent, bool)
	UpdateWithProposal(
		proposal *maxbftpb.ProposalData, isNextViewLeader bool) (*timeservice.TimerEvent, bool)
	UpdateWithView(view uint64) bool
	AddEvent(stateType maxbftpb.ConsStateType) *timeservice.TimerEvent
	GetMonitorEvent() *timeservice.TimerEvent
}

var _ PaceMaker = &DefaultPaceMaker{}

// DefaultPaceMaker is the default implementation of PaceMaker interface
type DefaultPaceMaker struct {
	// ensure that the pacemaker will not start twice
	started atomic.Value

	// current view in local node's state machine
	curView uint64

	// controller of the timers
	timeoutControl *timeservice.TimerService

	// forest
	fork forest.Forester

	// base round timeout
	roundTimeout time.Duration

	// timeout interval
	roundTimeoutInterval time.Duration

	// max timeout
	maxTimeout time.Duration

	logger protocol.Logger

	// record the last timerEvent, because wal file maybe has many NewView message,
	// get lastTimerEvent from timeoutControl is incorrect because many message
	// from channel to be consumer that need time.
	lastTimerEventInWal *timeservice.TimerEvent
}

// NewPaceMaker initials and returns a new DefaultPaceMaker object
func NewPaceMaker(curView uint64, fork forest.Forester, roundTime, roundTimeInterval,
	maxTime uint64, log protocol.Logger) PaceMaker {
	timerService := timeservice.NewTimerService(log)
	pacemaker := &DefaultPaceMaker{
		started:        atomic.Value{},
		curView:        curView,
		fork:           fork,
		timeoutControl: timerService,
		logger:         log,
	}
	if roundTime != 0 {
		pacemaker.roundTimeout = time.Duration(roundTime)
	} else {
		pacemaker.roundTimeout = utils.DefaultRoundTimeout
	}
	if roundTimeInterval != 0 {
		pacemaker.roundTimeoutInterval = time.Duration(roundTimeInterval)
	} else {
		pacemaker.roundTimeoutInterval = utils.DefaultRoundTimeoutInterval
	}
	if maxTime != 0 {
		pacemaker.maxTimeout = time.Duration(maxTime)
	} else {
		pacemaker.maxTimeout = utils.DefaultMaxRoundTimeout
	}
	pacemaker.started.Store(false)
	return pacemaker
}

// Start starts the pacemaker
func (pm *DefaultPaceMaker) Start() {
	if started, ok := pm.started.Load().(bool); ok && started {
		pm.logger.Warnf("pacemaker is already started")
		return
	}
	// record that the pacemaker was started
	pm.started.Store(true)

	// start the controller of timers
	go pm.timeoutControl.Start()
}

// Stop the service of paceMaker
func (pm *DefaultPaceMaker) Stop() {
	pm.timeoutControl.Stop()
	pm.logger.Infof("paceMaker close in view %d", pm.curView)
}

// CurView returns the current view of consensus
func (pm *DefaultPaceMaker) CurView() uint64 {
	return pm.curView
}

// OnTimeout push the local node state machine by a timer event
// and process the timer event
func (pm *DefaultPaceMaker) OnTimeout(event *timeservice.TimerEvent) *timeservice.TimerEvent {
	pm.logger.Infof("pacemaker got timeout event:%+v", event)

	// push the local node state machine
	pm.curView++

	// increase the global timer event timeout period
	//RoundTimeout = increaseRoundTimeout(RoundTimeout)
	roundTimeout := pm.getEventTimeout()

	// construct a new timer event and add to the controller
	event = &timeservice.TimerEvent{
		View:     pm.curView,
		Duration: roundTimeout,
		Type:     event.Type,
	}
	pm.timeoutControl.AddEvent(event)
	pm.logger.Debugf("pacemaker add event: %+v", event)
	return event
}

// TimeoutChannel get the timeout event channel
func (pm *DefaultPaceMaker) TimeoutChannel() <-chan *timeservice.TimerEvent {
	return pm.timeoutControl.GetFiredCh()
}

// UpdateWithView push the local node state machine by a view when replay wal
func (pm *DefaultPaceMaker) UpdateWithView(view uint64) bool {
	updated := false
	if view > pm.curView {
		pm.curView = view
		updated = true
	}

	// get the global timer event timeout period
	pm.lastTimerEventInWal = pm.AddEvent(maxbftpb.ConsStateType_PACEMAKER)
	return updated
}

// UpdateWithQc push the local node state machine by a qc
func (pm *DefaultPaceMaker) UpdateWithQc(qc *maxbftpb.QuorumCert) (*timeservice.TimerEvent, bool) {
	pm.logger.Debugf("pacemaker: updateWithQc start. "+
		"qc: %d:%d:%x", qc.Votes[0].Height, qc.Votes[0].View, qc.Votes[0].BlockId)
	view := qc.Votes[0].View
	if view < pm.curView {
		return nil, false
	}
	pm.curView = view + 1

	// get the global timer event timeout period
	event := pm.AddEvent(maxbftpb.ConsStateType_PACEMAKER)
	return event, true
}

// UpdateWithProposal push the local node state machine by a proposal
func (pm *DefaultPaceMaker) UpdateWithProposal(
	proposal *maxbftpb.ProposalData, isNextViewLeader bool) (*timeservice.TimerEvent, bool) {
	header := proposal.Block.Header
	pm.logger.Debugf("pacemaker: updateWithProposal start. qc: [%d:%d:%x]",
		header.BlockHeight, proposal.View, header.BlockHash)
	var (
		qc      = proposal.JustifyQc
		updated = false
	)

	// push local node state machine by the qc in the proposal
	if qc.Votes[0].View > pm.curView {
		pm.curView = qc.Votes[0].View + 1
		updated = true
	}

	// check the proposal view
	if pm.curView != proposal.View {
		pm.logger.Warnf("receive proposal on wrong view. proposal view: %d, curView:%d",
			proposal.View, pm.curView)
		return nil, updated
	}

	// follower: push the local node state machine,
	// then construct a pacemaker event and add it to timer controller
	if !isNextViewLeader {
		pm.curView = proposal.View + 1
		updated = true
		event := pm.AddEvent(maxbftpb.ConsStateType_PACEMAKER)
		return event, updated
	}

	// leader: construct a vote_collect event and add it to timer controller,
	// without pushing state machine
	return pm.AddEvent(maxbftpb.ConsStateType_VOTE_COLLECT), false
}

// AddEvent adds an event to the time_service
func (pm *DefaultPaceMaker) AddEvent(eventType maxbftpb.ConsStateType) *timeservice.TimerEvent {
	roundTimeout := pm.getEventTimeout()
	event := &timeservice.TimerEvent{
		View:     pm.curView,
		Duration: roundTimeout,
		Type:     eventType,
	}
	pm.timeoutControl.AddEvent(event)
	pm.logger.Debugf("pacemaker add event: %+v", event)
	return event
}

func (pm *DefaultPaceMaker) getEventTimeout() time.Duration {
	var timeout time.Duration
	if pm.curView-pm.fork.FinalView() <= 4 {
		timeout = time.Duration(pm.roundTimeout.Nanoseconds())
	} else {
		timeout = time.Duration(pm.roundTimeout.Nanoseconds() + pm.roundTimeoutInterval.Nanoseconds()*
			int64(pm.curView-pm.fork.FinalView()-4))
	}
	if timeout > pm.maxTimeout {
		timeout = pm.maxTimeout
	}
	return timeout * time.Millisecond
}

// GetMonitorEvent export timer event of the monitor that only be called in replay wal mode.
func (pm *DefaultPaceMaker) GetMonitorEvent() *timeservice.TimerEvent {
	return pm.lastTimerEventInWal
}
