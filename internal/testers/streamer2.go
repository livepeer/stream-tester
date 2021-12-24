package testers

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

// IgnoreNoCodecError ...
var IgnoreNoCodecError bool

// StartDelayBetweenGroups delay between start of group of streams
var StartDelayBetweenGroups = 2 * time.Second

type (
	Streamer2Options struct {
		WowzaMode              bool
		MistMode               bool
		Save                   bool
		FailIfTranscodingStops bool
		PrintStats             bool
	}

	StartTestFunc func(ctx context.Context, mediaURL string, waitForTarget time.Duration, opts Streamer2Options) Finite

	// streamer2 is used for running continious tests against Wowza servers
	streamer2 struct {
		finite
		Streamer2Options
		uploader        *rtmpStreamer
		downloader      *m3utester2
		additionalTests []StartTestFunc
		err             error
	}
)

// NewStreamer2 returns new streamer2
func NewStreamer2(pctx context.Context, opts Streamer2Options, additionalTests ...StartTestFunc) model.Streamer2 {
	ctx, cancel := context.WithCancel(pctx)
	return &streamer2{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		Streamer2Options: opts,
		additionalTests:  additionalTests,
	}
}

func (sr *streamer2) Err() error {
	return sr.err
}

func (sr *streamer2) Stats() (model.Stats1, error) {
	var stats model.Stats1
	if sr.downloader != nil {
		stats = sr.downloader.Stats()
	}
	stats.Finished = sr.Finished()
	return stats, sr.globalError
}

// StartStreaming starts streaming into rtmpIngestURL and reading back from mediaURL.
// Will stream indefinitely if timeToStream is -1, until error occurs.
// Does not exit until error or stream ends.
func (sr *streamer2) StartStreaming(sourceFileName string, rtmpIngestURL, mediaURL string, waitForTarget, timeToStream time.Duration) {
	if sr.uploader != nil {
		glog.Fatal("Already streaming")
	}
	// check if we can make TCP connection to RTMP target
	if err := utils.WaitForTCP(waitForTarget, rtmpIngestURL); err != nil {
		sr.fatalEnd(err)
		return
	}

	sm := newSegmentsMatcher()
	// sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, sr.wowzaMode)
	sr.uploader = newRtmpStreamer(sr.ctx, rtmpIngestURL, sourceFileName, sourceFileName, nil, nil, false, sm)
	// if timeToStream == 0 {
	// 	timeToStream = -1
	// }
	go func() {
		sr.uploader.StartUpload(sourceFileName, rtmpIngestURL, timeToStream, waitForTarget)
	}()
	sr.downloader = newM3utester2(sr.ctx, mediaURL, sr.WowzaMode, sr.MistMode,
		sr.FailIfTranscodingStops, sr.Save, sr.PrintStats, waitForTarget, sm, false) // starts to download at creation
	tests := []Finite{sr.downloader}
	for _, startFunc := range sr.additionalTests {
		tests = append(tests, startFunc(sr.ctx, mediaURL, waitForTarget, sr.Streamer2Options))
	}
	go func() {
		testsDone := onAnyDone(sr.ctx, tests)
		for {
			select {
			case <-sr.ctx.Done():
				return
			case test := <-testsDone:
				if err := test.GlobalErr(); err != nil {
					sr.fatalEnd(err)
					return
				}
			}
		}
	}()
	started := time.Now()
	<-sr.uploader.Done()
	if sr.uploader.Err() != nil {
		sr.err = sr.uploader.Err()
	}
	msg := fmt.Sprintf(`Streaming stopped after %s err=%v`, time.Since(started), sr.err)
	if messengerStats {
		messenger.SendMessage(msg)
	}
	sr.cancel()
}

func onAnyDone(ctx context.Context, finites []Finite) <-chan Finite {
	finished := make(chan Finite, len(finites))
	for _, f := range finites {
		go func(f Finite) {
			select {
			case <-f.Done():
				finished <- f
			case <-ctx.Done():
			}
		}(f)
	}
	return finished
}

// StartPulling pull arbitrary HLS stream and report found errors
/*
func (sr *streamer2) StartPulling(mediaURL string) {
	sr.downloader = newM3utester2(sr.ctx, mediaURL, sr.wowzaMode, sr.mistMode, 0, nil) // starts to download at creation
	started := time.Now()
	<-sr.downloader.Done()
	msg := fmt.Sprintf(`Streaming stopped after %s`, time.Since(started))
	messenger.SendMessage(msg)
}
*/
