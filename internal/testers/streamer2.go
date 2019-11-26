package testers

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
	"time"
)

type (
	streamer2 struct {
		uploader   *rtmpStreamer
		downloader *m3utester2
		eof        chan struct{}
		wowzaMode  bool
	}
)

// NewStreamer2 returns new streamer2
func NewStreamer2(wowzaMode bool) model.Streamer2 {
	return &streamer2{eof: make(chan struct{}), wowzaMode: wowzaMode}
}

// StartStreaming starts streaming into rtmpIngestURL and reading back from mediaURL.
// Will stream indefinitely, until error occurs.
// Does not exit until error.
func (sr *streamer2) StartStreaming(sourceFileName string, rtmpIngestURL, mediaURL string, waitForTarget time.Duration) {
	if sr.uploader != nil {
		glog.Fatal("Already streaming")
	}
	// sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, sr.wowzaMode)
	sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, false)
	go func() {
		sr.uploader.startUpload(sourceFileName, rtmpIngestURL, -1, waitForTarget)
	}()
	sr.downloader = newM3utester2(mediaURL, sr.wowzaMode, sr.eof, waitForTarget) // starts to download at creation
	started := time.Now()
	<-sr.eof
	msg := fmt.Sprintf(`Streaming stopped after %s`, time.Since(started))
	messenger.SendMessage(msg)
}
