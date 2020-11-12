package testers

/*

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
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
	glog.Info(msg)
	down := newM3utester2(ip.ctx, ip.url, ip.wowza, ip.mist, time.Minute, nil) // starts to download at creation
	started := time.Now()
	<-down.Done()
	msg = fmt.Sprintf("Done pulling from %s after %s", ip.url, time.Since(started))
	glog.Info(msg)

		// down := newM3UTester(ip.ctx, nil, ip.wowza, false, false, true, ip.save, nil, nil, "")
		// messenger.SendMessage(msg)
		// down.Start(ip.url)
		// <-down.ctx.Done()
		// msg = fmt.Sprintf("Done pulling from %s", ip.url)
		// messenger.SendMessage(msg)
}

*/
