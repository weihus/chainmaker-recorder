package recorderfile

import (
	"fmt"
	"runtime"
)

func safeGoroutine(f func() error, resultC chan<- error) {
	var err error
	defer func() {
		if pErr := recover(); pErr != nil {
			fmt.Printf("[recorder] got panic: %+v", pErr)
			if err == nil {
				err = fmt.Errorf("[recorder] got panic: %+v", pErr)
			}
		}
		if resultC != nil {
			resultC <- err
		}
	}()
	err = f()
}

//getCaller reports file:line number:function name information about function invocation of the parent function
func getCaller() string {
	funcName, fileName, lineNo, _ := caller(3)
	return fmt.Sprintf("%s:%d:%s", fileName, lineNo, funcName)
}

func caller(skip int) (funcName, fileName string, lineNo int, ok bool) {
	pc, file, line, ok := runtime.Caller(skip)
	return runtime.FuncForPC(pc).Name(), file, line, ok
}
