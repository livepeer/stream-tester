package testers

import (
	"context"
)

// TODO: Implement this for real

type (
	streamHealth struct {
		finite
	}
)

func NewStreamHealth(parent context.Context) Finite {
	ctx, cancel := context.WithCancel(parent)
	return &streamHealth{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
	}
}
