package testers

import (
	"time"

	"github.com/golang/glog"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/av/pktque"
	"github.com/nareix/joy4/format/rtmp"
)

var segLen = 2 * time.Second

// rtmpStreamer streams one video file to RTMP server
type rtmpStreamer struct {
	ingestURL       string
	counter         *segmentsCounter
	skippedSegments int
	connectionLost  bool
	active          bool
}

// source is local file name for now
func newRtmpStreamer(ingestURL, source string) *rtmpStreamer {
	return &rtmpStreamer{
		ingestURL:       ingestURL,
		counter:         newSegmentsCounter(segLen),
		skippedSegments: 1, // Broadcaster always skips first segment, but can skip more - this will be corrected when first
		// segment downloaded back
	}
}

func (rs *rtmpStreamer) startUpload(fn, manifestID string) {
	file, err := avutil.Open(fn)
	if err != nil {
		glog.Fatal(err)
	}
	rs.active = true
	defer func() {
		rs.active = false
	}()

	// pio.RecommendBufioSize = 1024 * 8
	// rtmp.Debug2 = true
	conn, err := rtmp.Dial("rtmp://localhost:1935/" + manifestID)
	if err != nil {
		glog.Fatal(err)
	}
	filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, rs.counter}

	demuxer := &pktque.FilterDemuxer{Demuxer: file, Filter: filters}
	err = avutil.CopyFile(conn, demuxer)
	if err != nil {
		glog.Error(err)
		rs.connectionLost = true
		file.Close()
		conn.Close()
		return
	}

	file.Close()
	// wait before closing connection, so we can recieve transcoded data
	// if we do not wait, last segment will be thrown out by broadcaster
	// with 'Session ended` error
	time.Sleep(8 * time.Second)
	conn.Close()
}
