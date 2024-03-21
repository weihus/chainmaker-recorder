/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package logger

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	api "chainmaker.org/chainmaker/protocol/v2"
)

// log level
const (
	levelDebug Level = "DEBUG"
	levelError Level = "ERROR"
	levelInfo  Level = "INFO"
	levelWarn  Level = "WARN"
	levelFatal Level = "FATAL"
	levelPanic Level = "PANIC"

	lnTemplate      = "%s [%s] %s %s"
	lnLabelTemplate = "%s [%s] [%s] %s %s"

	timeFormat = "2006-01-02 15:04:05.000"
)

// Level .
type Level string

var _ api.Logger = (*LogPrinter)(nil)

// LogPrinter information Definition
type LogPrinter struct {
	Label string
}

// NewLogPrinter create a new log printer object
func NewLogPrinter(label string) *LogPrinter {
	return &LogPrinter{
		Label: label,
	}
}

// location get information about the location of the print log, such as directory files, etc.
func (l *LogPrinter) location() string {
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		return "<unknown>"
	}
	dir, fileName := filepath.Split(file)
	dir = filepath.Base(dir)
	strBuilder := strings.Builder{}
	strBuilder.WriteString(dir)
	strBuilder.WriteString("/")
	strBuilder.WriteString(fileName)
	strBuilder.WriteString(":")
	strBuilder.WriteString(strconv.Itoa(line))
	return strBuilder.String()
}

// createContent concatenate the content of the log to output, return a string
func (l *LogPrinter) createContent(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}
	builder := strings.Builder{}
	for i := range args {
		builder.WriteString(fmt.Sprintf("%v ", args[i]))
	}
	return builder.String()
}

// println newline print,unformatted output
func (l *LogPrinter) println(level Level, args ...interface{}) {
	time := time.Now().Format(timeFormat)
	content := l.createContent(args...)
	if l.Label == "" {
		fmt.Println(fmt.Sprintf(lnTemplate, time, level, l.location(), content))
	} else {
		fmt.Println(fmt.Sprintf(lnLabelTemplate, time, level, l.Label, l.location(), content))
	}
	if level == levelPanic || level == levelFatal {
		panic(content)
	}
}

// printf formatted output
func (l *LogPrinter) printf(level Level, format string, args ...interface{}) {
	time := time.Now().Format(timeFormat)
	content := fmt.Sprintf(format, args...)
	if l.Label == "" {
		fmt.Println(fmt.Sprintf(lnTemplate, time, level, l.location(), content))
	} else {
		fmt.Println(fmt.Sprintf(lnLabelTemplate, time, level, l.Label, l.location(), content))
	}
	if level == levelPanic || level == levelFatal {
		panic(content)
	}
}

// Debug level log information output
func (l *LogPrinter) Debug(args ...interface{}) {
	l.println(levelDebug, args...)
}

// Debugf level log information formatted output
func (l *LogPrinter) Debugf(format string, args ...interface{}) {
	l.printf(levelDebug, format, args...)
}

// Error level log information output
func (l *LogPrinter) Error(args ...interface{}) {
	l.println(levelError, args...)
}

// Errorf Error level log information formatted output
func (l *LogPrinter) Errorf(format string, args ...interface{}) {
	l.printf(levelError, format, args...)
}

// Info level log information output
func (l *LogPrinter) Info(args ...interface{}) {
	l.println(levelInfo, args...)
}

// Infof Info level log information formatted output
func (l *LogPrinter) Infof(format string, args ...interface{}) {
	l.printf(levelInfo, format, args...)
}

// Panic level log information output
func (l *LogPrinter) Panic(args ...interface{}) {
	l.println(levelPanic, args...)
}

// Panicf Panic level log information formatted output
func (l *LogPrinter) Panicf(format string, args ...interface{}) {
	l.printf(levelPanic, format, args...)
}

// Warn level log information output
func (l *LogPrinter) Warn(args ...interface{}) {
	l.println(levelWarn, args...)
}

// Warnf Warn level log information formatted output
func (l *LogPrinter) Warnf(format string, args ...interface{}) {
	l.printf(levelWarn, format, args...)
}

// Debugw .
func (l *LogPrinter) Debugw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Errorw .
func (l *LogPrinter) Errorw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Fatal level log information output
func (l *LogPrinter) Fatal(args ...interface{}) {
	panic("implement me")
}

// Fatalf Fatal level log information formatted output
func (l *LogPrinter) Fatalf(format string, args ...interface{}) {
	panic("implement me")
}

// Fatalw .
func (l *LogPrinter) Fatalw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Infow .
func (l *LogPrinter) Infow(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Panicw .
func (l *LogPrinter) Panicw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// Warnw .
func (l *LogPrinter) Warnw(msg string, keysAndValues ...interface{}) {
	panic("implement me")
}

// DebugDynamic .
func (l *LogPrinter) DebugDynamic(getStr func() string) {
	panic("implement me")
}

// InfoDynamic .
func (l *LogPrinter) InfoDynamic(getStr func() string) {
	panic("implement me")
}
