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

	// streamer2 is used for running continious tests against Wowza servers
	streamer2 struct {
		finite
		Streamer2Options
		uploader   *rtmpStreamer
		downloader *m3utester2
		err        error
	}
)

// NewStreamer2 returns new streamer2
func NewStreamer2(pctx context.Context, opts Streamer2Options) model.Streamer2 {
	ctx, cancel := context.WithCancel(pctx)
	return &streamer2{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		Streamer2Options: opts,
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
		glog.Info(err)
		sr.globalError = err
		messenger.SendFatalMessage(err.Error())
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
	go func() {
		select {
		case <-sr.ctx.Done():
		case <-sr.downloader.Done():
			sr.globalError = sr.downloader.globalError
			// media downloader exited (probably with error), stop streaming
			sr.cancel()
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
