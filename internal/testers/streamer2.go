package testers

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
)

// IgnoreNoCodecError ...
var IgnoreNoCodecError bool

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
	// check if we can make TCP connection to RTMP target
	if err := sr.waitForTCP(waitForTarget, rtmpIngestURL); err != nil {
		glog.Info(err)
		messenger.SendFatalMessage(err.Error())
		return
	}

	sm := newsementsMatcher()
	// sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, sr.wowzaMode)
	sr.uploader = newRtmpStreamer(rtmpIngestURL, sourceFileName, nil, nil, sr.eof, false, sm)
	go func() {
		sr.uploader.startUpload(sourceFileName, rtmpIngestURL, -1, waitForTarget)
	}()
	sr.downloader = newM3utester2(mediaURL, sr.wowzaMode, sr.eof, waitForTarget, sm) // starts to download at creation
	started := time.Now()
	<-sr.eof
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
