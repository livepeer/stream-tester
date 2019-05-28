package testers

import (
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/gosuri/uiprogress"
	"github.com/nareix/joy4/av"
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
	done            chan struct{}
	file            av.DemuxCloser
}

// source is local file name for now
func newRtmpStreamer(ingestURL, source string, bar *uiprogress.Bar, done chan struct{}) *rtmpStreamer {
	return &rtmpStreamer{
		done:            done,
		ingestURL:       ingestURL,
		counter:         newSegmentsCounter(segLen, bar),
		skippedSegments: 1, // Broadcaster always skips first segment, but can skip more - this will be corrected when first
		// segment downloaded back
	}
}

// GetNumberOfSegments returns number of segments in video file
func GetNumberOfSegments(fileName string) int {
	file, err := avutil.Open(fileName)
	if err != nil {
		glog.Fatal(err)
	}
	sc := newSegmentsCounter(segLen, nil)
	filters := pktque.Filters{sc}
	src := &pktque.FilterDemuxer{Demuxer: file, Filter: filters}
	var streams []av.CodecData
	var videoidx, audioidx int
	if streams, err = src.Streams(); err != nil {
		glog.Fatal("Can't count segments in source file")
	}
	for i, st := range streams {
		if st.Type().IsAudio() {
			audioidx = i
		}
		if st.Type().IsVideo() {
			videoidx = i
		}
	}
	glog.Infof("Video index: %d, audio index: %d", videoidx, audioidx)
	for {
		var pkt av.Packet
		if pkt, err = src.ReadPacket(); err != nil {
			if err == io.EOF {
				break
			}
			glog.Infof("Paket time %s", pkt.Time)
			sc.ModifyPacket(&pkt, streams, videoidx, audioidx)
		}
	}
	return sc.segments - 1
}

func (rs *rtmpStreamer) startUpload(fn, rtmpURL string) {
	var err error
	rs.file, err = avutil.Open(fn)
	if err != nil {
		glog.Fatal(err)
	}
	rs.active = true
	defer func() {
		rs.active = false
	}()

	// pio.RecommendBufioSize = 1024 * 8
	// rtmp.Debug2 = true
	// conn, err := rtmp.Dial("rtmp://localhost:1935/" + manifestID)
	conn, err := rtmp.Dial(rtmpURL)
	if err != nil {
		glog.Fatal(err)
	}
	filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, rs.counter}

	demuxer := &pktque.FilterDemuxer{Demuxer: rs.file, Filter: filters}
	err = avutil.CopyFile(conn, demuxer)
	if err != nil {
		glog.Error(err)
		rs.connectionLost = true
		rs.file.Close()
		conn.Close()
		time.Sleep(4 * time.Second)
		close(rs.done)
		return
	}

	rs.file.Close()
	// wait before closing connection, so we can recieve transcoded data
	// if we do not wait, last segment will be thrown out by broadcaster
	// with 'Session ended` error
	time.Sleep(8 * time.Second)
	conn.Close()
	close(rs.done)
}
