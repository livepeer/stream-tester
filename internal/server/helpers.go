package server

import (
	"fmt"
	"time"
)

// ParseStreamDurationArgument parses desired stream duration passed in command line
func ParseStreamDurationArgument(stime string) (time.Duration, error) {
	streamDuration, err := time.ParseDuration(stime)
	if err != nil {
		return streamDuration, fmt.Errorf("error parsing time: %v", err)
	}
	if streamDuration < 0 {
		return streamDuration, fmt.Errorf("time to stream should be positive")
	}
	if streamDuration == 0 {
		return streamDuration, fmt.Errorf("time to stream should be greater than zero")
	}
	return streamDuration, nil
}
