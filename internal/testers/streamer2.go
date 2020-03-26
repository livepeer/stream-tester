package testers

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

// IgnoreNoCodecError ...
var IgnoreNoCodecError bool

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
func (sr *streamer2) StartStreaming(sourceFileName string, rtmpIngestURL, mediaURL string, waitForTarget time.Duration) {
	if sr.uploader != nil {
		glog.Fatal("Already streaming")
	}
	// check if we can make TCP connection to RTMP target
	if err := sr.waitForTCP(waitForTarget, rtmpIngestURL); err != nil {
		glog.Info(err)
		messenger.SendFatalMessage(err.Error())
		return
	}

	sm := newsementsMatcher()
	// sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, sr.wowzaMode)
	sctx, scancel := context.WithCancel(sr.ctx)
	sr.uploader = newRtmpStreamer(sctx, scancel, rtmpIngestURL, sourceFileName, sourceFileName, nil, nil, false, sm)
	go func() {
		sr.uploader.StartUpload(sourceFileName, rtmpIngestURL, -1, waitForTarget)
	}()
	sr.downloader = newM3utester2(sr.ctx, sr.cancel, mediaURL, sr.wowzaMode, sr.mistMode, waitForTarget, sm) // starts to download at creation
	started := time.Now()
	<-sctx.Done()
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

func (sr *streamer2) waitForTCP(waitForTarget time.Duration, rtmpIngestURL string) error {
	var u *url.URL
	var err error
	if u, err = url.Parse(rtmpIngestURL); err != nil {
		return err
	}
	dailer := net.Dialer{Timeout: 2 * time.Second}
	started := time.Now()
	for {
		if _, err = dailer.Dial("tcp", u.Host); err != nil {
			time.Sleep(4 * time.Second)
			if time.Since(started) > waitForTarget {
				return fmt.Errorf("Can't connect to '%s' for more than %s", rtmpIngestURL, waitForTarget)
			}
			continue
		}
		break
	}
	return nil
}
