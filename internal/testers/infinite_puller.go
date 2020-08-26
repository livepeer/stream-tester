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
	ctx   context.Context
}

// NewInfinitePuller ...
func NewInfinitePuller(ctx context.Context, url string, save, wowza bool) model.InfinitePuller {
	return &infinitePuller{
		ctx:   ctx,
		url:   url,
		save:  save,
		wowza: wowza,
	}
}

func (ip *infinitePuller) Start() {
	/*
		file, err := avutil.Open("media_w1620423594_b1487091_59.ts-00059.ts")
		fmt.Printf("file type %T\n", file)
		// hd := file.(*avutil.HandlerDemuxer)
		// fmt.Printf("%T\n", hd.Demuxer)
		if err != nil {
			panic(err)
		}
	*/

	// var sentTimesMap *utils.SyncedTimesMap
	down := newM3UTester(ip.ctx, nil, ip.wowza, false, false, true, ip.save, nil, nil, "")
	// go findSkippedSegmentsNumber(up, down)
	// sr.downloaders = append(sr.downloaders, down)
	msg := fmt.Sprintf("Starting to pull infinite stream from %s", ip.url)
	messenger.SendMessage(msg)
	down.Start(ip.url)
	<-down.ctx.Done()
	msg = fmt.Sprintf("Done pulling from %s", ip.url)
	messenger.SendMessage(msg)
}
