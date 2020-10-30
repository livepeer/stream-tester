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
	// streamer2 is used for running continious tests against Wowza servers
	streamer2 struct {
		ctx        context.Context
		cancel     context.CancelFunc
		uploader   *rtmpStreamer
		downloader *m3utester2
		wowzaMode  bool
		mistMode   bool
	}
)

// NewStreamer2 returns new streamer2
func NewStreamer2(ctx context.Context, cancel context.CancelFunc, wowzaMode, mistMode bool) model.Streamer2 {
	return &streamer2{
		ctx:       ctx,
		cancel:    cancel,
		wowzaMode: wowzaMode,
		mistMode:  mistMode,
	}
}

// StartStreaming starts streaming into rtmpIngestURL and reading back from mediaURL.
// Will stream indefinitely, until error occurs.
// Does not exit until error.
func (sr *streamer2) StartStreaming(sourceFileName string, rtmpIngestURL, mediaURL string, waitForTarget, timeToStream time.Duration) {
	if sr.uploader != nil {
		glog.Fatal("Already streaming")
	}
	// check if we can make TCP connection to RTMP target
	if err := utils.WaitForTCP(waitForTarget, rtmpIngestURL); err != nil {
		glog.Info(err)
		messenger.SendFatalMessage(err.Error())
		return
	}

	sm := newsementsMatcher()
	// sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, sr.wowzaMode)
	sctx, scancel := context.WithCancel(sr.ctx)
	sr.uploader = newRtmpStreamer(sctx, scancel, rtmpIngestURL, sourceFileName, sourceFileName, nil, nil, false, sm)
	if timeToStream == 0 {
		timeToStream = -1
	}
	go func() {
		sr.uploader.StartUpload(sourceFileName, rtmpIngestURL, timeToStream, waitForTarget)
	}()
	sr.downloader = newM3utester2(sr.ctx, sr.cancel, mediaURL, sr.wowzaMode, sr.mistMode, waitForTarget, sm) // starts to download at creation
	started := time.Now()
	<-sctx.Done()
	sr.cancel()
	msg := fmt.Sprintf(`Streaming stopped after %s`, time.Since(started))
	messenger.SendMessage(msg)
}

// StartPulling pull arbitrary HLS stream and report found errors
func (sr *streamer2) StartPulling(mediaURL string) {
	sctx, scancel := context.WithCancel(sr.ctx)
	sr.downloader = newM3utester2(sctx, scancel, mediaURL, sr.wowzaMode, sr.mistMode, 0, nil) // starts to download at creation
	started := time.Now()
	<-sctx.Done()
	msg := fmt.Sprintf(`Streaming stopped after %s`, time.Since(started))
	messenger.SendMessage(msg)
}
