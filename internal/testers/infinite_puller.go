package testers

import (
	"context"
	"fmt"

	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

type infinitePuller struct {
	url  string
	save bool
}

// NewInfinitePuller ...
func NewInfinitePuller(url string, save bool) model.InfinitePuller {
	return &infinitePuller{
		url:  url,
		save: save,
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
	down := newM3UTester(context.Background(), nil, true, false, false, true, ip.save, nil, nil, "")
	// go findSkippedSegmentsNumber(up, down)
	// sr.downloaders = append(sr.downloaders, down)
	msg := fmt.Sprintf("Starting to pull infinite stream from %s", ip.url)
	messenger.SendMessage(msg)
	down.Start(ip.url)
}
