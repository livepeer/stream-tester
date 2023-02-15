package vodtester

import (
	"context"
	"github.com/livepeer/stream-tester/internal/app/common"
)

type (
	// IContinuousVodTester ...
	IContinuousVodTester interface {
		// Start test. Blocks until error.
		Start(fileName string, vodImportUrl string) error
	}

	continuousVodTester struct {
		ct   common.IContinuousTester
		opts common.TesterOptions
	}
)

// NewContinuousVodTester returns new object
func NewContinuousVodTester(gctx context.Context, opts common.ContinuousTesterOptions) IContinuousVodTester {
	return &continuousVodTester{
		ct:   common.NewContinuousTester(gctx, opts, "VOD"),
		opts: opts.TesterOptions,
	}
}

func (cvt *continuousVodTester) Start(fileName string, vodImportUrl string) error {
	start := func(ctx context.Context) error {
		vt := NewVodTester(ctx, cvt.opts)
		return vt.Start(fileName, vodImportUrl)
	}
	return cvt.ct.Start(start)
}
