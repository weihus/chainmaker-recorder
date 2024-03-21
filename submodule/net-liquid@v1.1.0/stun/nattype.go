package stun

// BehaviorType nat Behavior Type
type BehaviorType int

const (
	// BehaviorTypeUnknown 0
	BehaviorTypeUnknown BehaviorType = iota
	// BehaviorTypeEndpoint 1
	BehaviorTypeEndpoint
	// BehaviorTypeAddr 2
	BehaviorTypeAddr
	// BehaviorTypeAddrAndPort 3
	BehaviorTypeAddrAndPort
)

// NATBehavior nat MappingType and FilteringType
type NATBehavior struct {
	// NAT MappingType
	MappingType BehaviorType
	// NAT FilteringType
	FilteringType BehaviorType
}

// NATDeviceType .
type NATDeviceType int

const (
	// NATTypeUnknown 0
	NATTypeUnknown NATDeviceType = iota
	// NATTypeFullCone 1
	NATTypeFullCone
	// NATTypeRestrictedCone 2
	NATTypeRestrictedCone
	// NATTypePortRestrictedCone 3
	NATTypePortRestrictedCone
	// NATTypeSymmetric 4
	NATTypeSymmetric
)
