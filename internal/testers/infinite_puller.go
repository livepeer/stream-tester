package testers

import (
	"context"
	"fmt"

	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

type infinitePuller struct {
	url   string
	save  bool
	wowza bool
	mist  bool
	ctx   context.Context
}

// NewInfinitePuller ...
func NewInfinitePuller(ctx context.Context, url string, save, wowza, mist bool) model.InfinitePuller {
	return &infinitePuller{
		ctx:   ctx,
		url:   url,
		save:  save,
		wowza: wowza,
		mist:  mist,
	}
}

func (ip *infinitePuller) Start() {
	msg := fmt.Sprintf("Starting to pull infinite stream from %s", ip.url)

	// var sentTimesMap *utils.SyncedTimesMap
	down := newM3UTester(ip.ctx, nil, ip.wowza, false, false, true, ip.save, nil, nil, "")
	// go findSkippedSegmentsNumber(up, down)
	// sr.downloaders = append(sr.downloaders, down)
	messenger.SendMessage(msg)
	down.Start(ip.url)
	<-down.ctx.Done()
	msg = fmt.Sprintf("Done pulling from %s", ip.url)
	messenger.SendMessage(msg)
}
