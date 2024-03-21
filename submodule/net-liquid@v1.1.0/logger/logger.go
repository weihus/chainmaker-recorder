/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package logger

import api "chainmaker.org/chainmaker/protocol/v2"

// NilLogger is a nil implementation of Logger interface.
// It will log nothing.
var NilLogger = &nilLogger{}

// Logger is an interface of net logger.
//type Logger interface {
//	Debug(args ...interface{})
//	Debugf(format string, args ...interface{})
//	Error(args ...interface{})
//	Errorf(format string, args ...interface{})
//	Info(args ...interface{})
//	Infof(format string, args ...interface{})
//	Panic(args ...interface{})
//	Panicf(format string, args ...interface{})
//	Warn(args ...interface{})
//	Warnf(format string, args ...interface{})
//}

var _ api.Logger = (*nilLogger)(nil)

// nilLogger nil logger object
type nilLogger struct{}

// Debug level log information output
func (n *nilLogger) Debug(args ...interface{}) {}

// Debugf level log information formatted output
func (n *nilLogger) Debugf(format string, args ...interface{}) {}

// Error level log information output
func (n *nilLogger) Error(args ...interface{}) {}

// Errorf Error level log information formatted output
func (n *nilLogger) Errorf(format string, args ...interface{}) {}

// Info level log information output
func (n *nilLogger) Info(args ...interface{}) {}

// Infof Info level log information formatted output
func (n *nilLogger) Infof(format string, args ...interface{}) {}

// Panic level log information output
func (n *nilLogger) Panic(args ...interface{}) {}

// Panicf Panic level log information formatted output
func (n *nilLogger) Panicf(format string, args ...interface{}) {}

// Warn level log information output
func (n *nilLogger) Warn(args ...interface{}) {}

// Warnf Warn level log information formatted output
func (n *nilLogger) Warnf(format string, args ...interface{}) {}

// Debugw .
func (n *nilLogger) Debugw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Errorw .
func (n *nilLogger) Errorw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Fatal level log information output
func (n *nilLogger) Fatal(args ...interface{}) {
	panic("implement me")
}

// Fatalf Fatal level log information formatted output
func (n *nilLogger) Fatalf(format string, args ...interface{}) {
	panic("implement me")
}

// Fatalw .
func (n *nilLogger) Fatalw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Infow .
func (n *nilLogger) Infow(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Panicw .
func (n *nilLogger) Panicw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Warnw .
func (n *nilLogger) Warnw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// DebugDynamic .
func (n *nilLogger) DebugDynamic(getStr func() string) {
	panic("implement me")
}

// InfoDynamic .
func (n *nilLogger) InfoDynamic(getStr func() string) {
	panic("implement me")
}
