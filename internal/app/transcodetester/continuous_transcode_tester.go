package transcodetester

import (
	"context"
	"github.com/livepeer/stream-tester/internal/app/common"
)

type (
	IContinuousTranscodeTester interface {
		// Start test. Blocks until error.
		Start(fileName, transcodeBucketUrl, transcodeW3sProof string) error
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

func (ctt *continuousTranscodeTester) Start(fileName, transcodeBucketUrl, transcodeW3sProof string) error {
	start := func(ctx context.Context) error {
		tt := NewTranscodeTester(ctx, ctt.opts)
		return tt.Start(fileName, transcodeBucketUrl, transcodeW3sProof)
	}
	return ctt.ct.Start(start)
}
