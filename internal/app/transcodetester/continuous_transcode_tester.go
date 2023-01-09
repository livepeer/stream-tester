package transcodetester

import (
	"context"
	"time"

	"github.com/livepeer/stream-tester/internal/app/common"
)

type (
	IContinuousTranscodeTester interface {
		// Start test. Blocks until error.
		Start(fileName string, transcodeBucketUrl string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error
	}

	continuousTranscodeTester struct {
		ct   common.IContinuousTester
		opts common.TesterOptions
	}
)

func NewContinuousTranscodeTester(gctx context.Context, opts common.ContinuousTesterOptions) IContinuousTranscodeTester {
	return &continuousTranscodeTester{
		ct:   common.NewContinuousTester(gctx, opts, "transcode"),
		opts: opts.TesterOptions,
	}
}

func (ctt *continuousTranscodeTester) Start(fileName string, transcodeBucketUrl string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error {
	start := func(ctx context.Context) error {
		tt := NewTranscodeTester(ctx, ctt.opts)
		return tt.Start(fileName, transcodeBucketUrl, taskPollDuration)
	}
	return ctt.ct.Start(start, testDuration, pauseBetweenTests)
}
