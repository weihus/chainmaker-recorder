package engine

// EventType event type that engine will be processed
type EventType int

const (
	// RestartSyncServiceEvent start the sync service
	RestartSyncServiceEvent EventType = iota
	// FetchProposalEvent fetch proposal event
	FetchProposalEvent
	// FetchVoteEvent fetch vote event
	FetchVoteEvent
)

// Event which the engine will be processed
type Event interface {
	Type() EventType
	Data() interface{}
}

// StartSyncService start sync service
type StartSyncService struct {
}

// NewStartSyncService init
func NewStartSyncService() *StartSyncService {
	return &StartSyncService{}
}

// Type the event type
func (s *StartSyncService) Type() EventType {
	return RestartSyncServiceEvent
}

// Data the event data
func (s *StartSyncService) Data() interface{} {
	return nil
}

// FetchProposal fetch proposal event
type FetchProposal struct {
	//	add field with metadata for voting
}

// NewFetchProposal init
func NewFetchProposal() *FetchProposal {
	return &FetchProposal{}
}

// Type the event type
func (f *FetchProposal) Type() EventType {
	return FetchProposalEvent
}

// Data the event data which should be processed
func (f *FetchProposal) Data() interface{} {
	panic("implement me")
}

// FetchVote fetch vote event
type FetchVote struct {
	//	add field with metadata for voting
}

// NewFetchVote init
func NewFetchVote() *FetchVote {
	return &FetchVote{}
}

// Type the event type
func (f *FetchVote) Type() EventType {
	return FetchVoteEvent
}

// Data the event data which should be processed
func (f *FetchVote) Data() interface{} {
	panic("implement me")
}
