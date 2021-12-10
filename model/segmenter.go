package model

import "time"

type HlsSegment struct {
	Err      error
	SeqNo    int
	Pts      time.Duration
	Duration time.Duration
	Data     []byte
}
