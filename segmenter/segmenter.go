package segmenter

import (
	"context"
	"io"
	"time"

	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/model"
)

func StartSegmenting(ctx context.Context, fileName string, stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) error {

	return testers.StartSegmenting(ctx, fileName, stopAtFileEnd, stopAfter, skipFirst, segLen, useWallTime, out)
}

func StartSegmentingR(ctx context.Context, reader io.ReadSeekCloser, stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) error {

	return testers.StartSegmentingR(ctx, reader, stopAtFileEnd, stopAfter, skipFirst, segLen, useWallTime, out)
}
